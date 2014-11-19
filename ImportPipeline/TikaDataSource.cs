using Bitmanager.ImportPipeline;
using Bitmanager.Core;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using System.Threading.Tasks;
using System.Xml;
using System.Net;
using System.IO;
using HtmlAgilityPack;
using System.Text.RegularExpressions;
using System.Web;
using System.Threading;
using Bitmanager.Elastic;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline
{
   public class TikaDS: Datasource
   {
      private String processName;
      public String UriBase {get; private set;}
      public String DbgStoreDir { get; private set; }
      private IDatasourceFeeder feeder;
      private String pingUrl;
      private int pingTimeout;
      private int abstractLength, abstractDelta;
      private int maxParallel;

      private AsyncRequestQueue workerQueue;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         processName = node.ReadStr("@tikaprocess");
         UriBase = node.ReadStr("@tikaurl");
         if (!UriBase.EndsWith("/")) UriBase += "/";
         feeder = ctx.CreateFeeder (node);
         abstractLength = node.ReadInt("abstract/@maxlength", 256);
         abstractDelta = node.ReadInt("abstract/@delta", 20);
         DbgStoreDir = node.ReadStr("dbgstore", null);
         if (DbgStoreDir != null)
         {
            DbgStoreDir = IOUtils.AddSlash(ctx.ImportEngine.Xml.CombinePath(DbgStoreDir));
            IOUtils.ForceDirectories(DbgStoreDir, true);
         }
         ctx.ImportLog.Log("dbgstore dir={0}", DbgStoreDir ?? "NULL");

         pingUrl = node.ReadStr("@pingurl", null);
         pingTimeout = node.ReadInt("@pingtimeout", 10000);
         maxParallel = node.ReadInt("@maxparallel", 1);
      }

      private DateTime previousRun;
      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         workerQueue = AsyncRequestQueue.Create(maxParallel);
         ctx.ImportLog.Log("TikaDS starting. maxparallel={0}, dbgstore={1}, Q={2}", maxParallel, DbgStoreDir, workerQueue);
         if (maxParallel >= 2 && ServicePointManager.DefaultConnectionLimit < maxParallel)
         {
            ctx.ImportLog.Log("Updating connectionLimit for {0} to {1}", ServicePointManager.DefaultConnectionLimit, maxParallel);
            ServicePointManager.DefaultConnectionLimit = maxParallel;
         }
         ensureTikaServiceStarted(ctx);
         previousRun = ctx.RunAdministrations.GetLastOKRunDate(ctx.DatasourceAdmin.Name);
         ctx.ImportLog.Log("Previous run was {0}.", previousRun);
         foreach (var elt in feeder)
         {
            try
            {
               importUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, e.Message + "\r\nUrl=" + elt.Element + ".");
            }
         }

         //Handle still queued workers
         while (true)
         {
            TikaAsyncWorker popped = pushPop(ctx, sink, null);
            if (popped == null) break;
            importUrl(ctx, sink, popped);
         }
      }

      private static ExistState toExistState (Object result)
      {
         if (result == null || !(result is ExistState)) return ExistState.NotExist;
         return (ExistState)result;
      }

      private TikaAsyncWorker pushPop(PipelineContext ctx, IDatasourceSink sink, TikaAsyncWorker newElt)
      {
         try
         {
            return (TikaAsyncWorker)((newElt == null) ? workerQueue.Pop() : workerQueue.PushAndOptionalPop(newElt));
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
            return null;
         }
      }
      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IDatasourceFeederElement elt)
      {
         TikaAsyncWorker worker = new TikaAsyncWorker (this, elt);
         String fileName = worker.FullElt.FileName;
         sink.HandleValue (ctx, "record/_start", fileName);
         sink.HandleValue(ctx, "record/lastmodutc", worker.LastModifiedUtc);
         sink.HandleValue(ctx, "record/virtualFilename", worker.FullElt.VirtualFileName);

         //Check if we need to convert this file
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            if ((ctx.ImportFlags & _ImportFlags.RetryErrors)==0 && worker.LastModifiedUtc < previousRun)
            {
               ctx.Skipped++;
               return;
            }
            ExistState existState = toExistState (sink.HandleValue(ctx, "record/_checkexist", null));
            if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
            {
               ctx.Skipped++;
               //ctx.ImportLog.Log("Skipped: {0}. Date={1}", worker.FullElt.VirtualFileName, worker.LastModifiedUtc);
               return;
            }
         }


         TikaAsyncWorker popped = pushPop (ctx, sink, worker);
         if (popped != null)
            importUrl(ctx, sink, popped);
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, TikaAsyncWorker worker)
      {
         String fileName = worker.FullElt.FileName;
         sink.HandleValue(ctx, "record/_start", fileName);
         sink.HandleValue(ctx, "record/lastmodutc", worker.LastModifiedUtc);
         sink.HandleValue(ctx, "record/virtualFilename", worker.FullElt.VirtualFileName);

         try
         {

            var htmlProcessor = worker.HtmlProcessor;
            if (worker.StoredAs != null) sink.HandleValue(ctx, "record/converted_file", worker.StoredAs);

            //Write html properties
            foreach (var kvp in htmlProcessor.Properties)
            {
               sink.HandleValue(ctx, "record/" + kvp.Key, kvp.Value);
            }

            //Add dummy type to recognize the errors
            //if (error)
            //   doc.AddField("content_type", "ConversionError");
            //if (htmlProcessor.IsTextMail)
            sink.HandleValue(ctx, "record/_istextmail", htmlProcessor.IsTextMail);
            sink.HandleValue(ctx, "record/_numparts", htmlProcessor.numParts);
            sink.HandleValue(ctx, "record/_numattachments", htmlProcessor.numAttachments);
            sink.HandleValue(ctx, "record/_filesize", worker.FileSize);
            sink.HandleValue(ctx, "record/shortcontent", htmlProcessor.GetAbstract(abstractLength, abstractDelta));

            sink.HandleValue(ctx, "record/head", htmlProcessor.GetInnerHead());
            sink.HandleValue(ctx, "record/content", htmlProcessor.GetInnerBody());

            sink.HandleValue(ctx, "record/_end", fileName);
            sink.HandleValue(ctx, "record", null);
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }

      private void ensureTikaServiceStarted(PipelineContext ctx)
      {
         ctx.ImportEngine.JavaHostCollection.EnsureStarted(this.processName);
         if (pingUrl == null)
         {
            ctx.ImportLog.Log("No pingurl specified. Just waiting for 10s.");
            Thread.Sleep(10000);
            return;
         }

         DateTime limit = DateTime.UtcNow.AddMilliseconds(pingTimeout);
         Uri uri = new Uri(pingUrl);
         Exception saved = null;
         using (WebClient client = new WebClient())
         {
            while (true)
            {
               try
               {
                  client.DownloadData(uri);
                  return;
               }
               catch (Exception err)
               {
                  saved = err;
               }
               if (DateTime.UtcNow >= limit) break;
            }
            throw new BMException(saved, "Tika service did not startup within {0}ms. LastErr={1}", pingTimeout, saved.Message);
         }
      }
   }


   public class HtmlProcessor
   {
      public int numParts;
      public int numAttachments;
      public bool IsTextMail;
      private readonly bool removeTitleNodes = false;
      private readonly bool removeMetaNodes = false;
      private static Logger logger = Logs.CreateLogger("tika", "htmlprocessor");
      private String _newHtml;
      public readonly List<KeyValuePair<String,String>> Properties;
      public readonly HtmlDocument Document;
      public HtmlNode HtmlNode {get; private set;}
      public HtmlNode BodyNode { get; private set; }
      public HtmlNode HeadNode { get; private set; }

      public HtmlProcessor(HtmlDocument doc)
      {
         Properties = new List<KeyValuePair<string, string>>();
         var docNode = doc.DocumentNode;
         BodyNode = docNode.SelectSingleNode("//body");
         if (BodyNode != null)
            HtmlNode = findParentHtmlNode(BodyNode, docNode);
         else
            HtmlNode = BodyNode = docNode;

         if (HtmlNode == null) goto EXIT_RTN;
         HeadNode = HtmlNode.SelectSingleNode("head");
         if (HeadNode == null) goto EXIT_RTN; ;

         processMetaNodes(HeadNode.SelectNodes("meta"));
         processTitleNodes(HeadNode.SelectNodes("title"));
         removeEmptyTextNodes(HeadNode.ChildNodes);
         undupMailNodes();
         removeEmptyTextNodes(BodyNode.SelectNodes("//text()"));
         computeNumAttachments();
      EXIT_RTN:
         Document = doc;
      }

      private bool isTextNode(HtmlNode mailNode)
      {
         HtmlNode child = mailNode.SelectSingleNode("*");
         if (child == null) return false;
         if (String.Equals("pre", child.Name, StringComparison.OrdinalIgnoreCase)) return true;
         if (!String.Equals("p", child.Name, StringComparison.OrdinalIgnoreCase)) return false;
         child = child.SelectSingleNode("*");
         if (child == null) return false;
         return String.Equals("pre", child.Name, StringComparison.OrdinalIgnoreCase);
      }

      public void undupMailNodes()
      {
         if (BodyNode == null) return;
         HtmlNodeCollection nodes = BodyNode.SelectNodes("div[@class='email-entry']");
         int N = nodes==null ? 0 : nodes.Count;
         numParts = N;
         switch (N)
         {
            case 0: 
               IsTextMail = isTextNode (BodyNode);
               return;
            case 1:
               IsTextMail = isTextNode (nodes[0]);
               return;
         }

         int maxIdx = -1;
         int maxCnt = 0;
         for (int i = 0; i < nodes.Count; i++)
         {
            int cnt = nodes[i].Descendants().Count();
            if (cnt <= maxCnt) continue;
            maxCnt = cnt;
            maxIdx = i;
         }

         IsTextMail = isTextNode (nodes[maxIdx]);
         for (int i = 0; i < nodes.Count; i++)
         {
            if (maxIdx==i) continue;
            BodyNode.RemoveChild(nodes[i], false);
         }
      }

      private void computeNumAttachments()
      {
         if (BodyNode == null) return;
         HtmlNodeCollection nodes = BodyNode.SelectNodes("//p[@class='email-attachment-name']");
         numAttachments = nodes == null ? 0 : nodes.Count;
      }

      public String GetInnerBody()
      {
         return (BodyNode == null) ? String.Empty : BodyNode.InnerHtml;
      }
      public String GetInnerHead()
      {
         return (HeadNode == null) ? String.Empty : HeadNode.InnerHtml;
      }
      public String GetNewHtml()
      {
         if (_newHtml != null) return _newHtml;

         return _newHtml = Document.DocumentNode.OuterHtml;
      }

      private static HtmlNode findParentHtmlNode(HtmlNode bodyNode, HtmlNode doc)
      {
         for (HtmlNode p = bodyNode.ParentNode; p != null; p = p.ParentNode)
         {
            if (p.Name.Equals("html", StringComparison.InvariantCultureIgnoreCase)) return p;
         }
         return doc;
      }

      public static String GetAbstractFromText(String text, int maxLength, int delta)
      {
         //StringBuilder sb = new StringBuilder();
         if (text==null || text.Length <= maxLength + delta) return text;

         int from = maxLength - delta;
         int to = maxLength + delta;

         //String inspect = txt.Substring(from, 2 * delta);

         int bestSentencePos = -1;
         int bestWordPos = -1;
         for (int i = from; i < to; i++)
         {
            switch (text[i])
            {
               default: continue;
               case '.':
               case '?':
               case '!': setBestPos(ref bestSentencePos, i, maxLength); continue;

               case ' ':
               case ';':
               case ',':
               case ':':
               case '\r':
               case '\n':
               case '\t': setBestPos(ref bestWordPos, i, maxLength); continue;
            }
         }
         if (bestSentencePos < 0)
         {
            bestSentencePos = (bestWordPos < 0) ? maxLength - 1 : bestWordPos;
         }
         //inspect = txt.Substring(0, bestSentencePos) + " \u2026";
         //return inspect;
         return text.Substring(0, bestSentencePos) + " \u2026";     //Add ellipsis
      }
      private static void setBestPos(ref int pos, int i, int maxLength)
      {
         int d1 = Math.Abs(maxLength - pos);
         int d2 = Math.Abs(maxLength - i);
         if (d2 < d1) pos = i;
      }

      public String GetAbstract(int maxLength, int delta)
      {
         return GetAbstractFromText(GetText(maxLength + delta), maxLength, delta);
      }


      private static readonly char[] TRIMCHARS = { ' ', '\t', '\r', '\n' };
      private static bool appendInnerText(StringBuilder bld, HtmlNode node, int maxLength = -1)
      {
         switch (node.NodeType)
         {
            case HtmlNodeType.Text:
               String s = HttpUtility.HtmlDecode (node.InnerText);
               if (bld.Length == 0)
               {
                  if (String.IsNullOrEmpty(s)) return true;
                  s = s.TrimStart(TRIMCHARS);
               }
               if (String.IsNullOrEmpty(s)) return true;
               bld.Append(s);
               if (maxLength > 0 && bld.Length >= maxLength) return false;
               return true;
            case HtmlNodeType.Comment: return true;
         }

         _nodeType nt = getNodeType(node);
         switch (nt)
         {
            case _nodeType.Unwanted: return true;
            case _nodeType.Block:
               if (bld.Length > 0) bld.Append(' ');
               break;
         }

         if (!node.HasChildNodes) return true;

         foreach (HtmlNode c in node.ChildNodes)
         {
            if (!appendInnerText(bld, c, maxLength)) return false;
         }
         if (nt == _nodeType.Block && bld.Length > 0) bld.Append(' ');
         return true;
      }

      private enum _nodeType { Unwanted, Block, Inline };
      private static _nodeType getNodeType(HtmlNode node)
      {
         String name = node.Name.ToLowerInvariant();
         switch (name)
         {
            case "object":
            case "script":
            case "style":
               return _nodeType.Unwanted;

            case "p":
            case "div":
            case "br":
            case "table":
            case "thead":
            case "td":
            case "tr":
            case "span":
               return _nodeType.Block;
         }
         return _nodeType.Inline;
      }

      public String GetText(HtmlNode n, int maxLength)
      {
         if (n == null) return null;
         StringBuilder bld = new StringBuilder();
         appendInnerText(bld, n, maxLength);

         int i = bld.Length - 1;
         for (; i >= 0; i--)
         {
            switch (bld[i])
            {
               case ' ':
               case '\r':
               case '\n':
               case '\t': continue;
            }
            break;
         }

         return i < 0 ? null : bld.ToString(0, i + 1);
      }
      public String GetText(int maxLength)
      {
         return GetText(BodyNode, maxLength);
      }



      private void processMetaNodes(HtmlNodeCollection metaNodes)
      {
         if (metaNodes == null) return;
         List<HtmlNode> list = toList(metaNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlNode node = list[i];
            addMetaData(node.GetAttributeValue("name", null), node.GetAttributeValue("content", null));
            if (removeMetaNodes) node.Remove(); 
         }
      }
      //String s = HttpUtility.HtmlDecode(node.InnerText);
      private void removeEmptyTextNodes(HtmlNodeCollection textNodes)
      {
         if (textNodes == null) return;
         List<HtmlNode> list = toList(textNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlTextNode txtNode = list[i] as HtmlTextNode;

            if (txtNode == null) continue;
//            logger.Log("textNode[{0}]={1} [{2}]", i, onlyWhiteSpace(txtNode.Text), txtNode.Text);
            if (onlyWhiteSpace(txtNode.Text))
               txtNode.Remove();
         }
      }

      private  bool onlyWhiteSpace(String txt, bool decoded=false)
      {
         if (String.IsNullOrEmpty(txt)) return true;

         for (int i = 0; i < txt.Length; i++)
         {
            switch (txt[i])
            {
               //case '&':
               //   if (decoded)
               //   {
               //      logger.Log("Non empty char={0:X}", (int)txt[i]);
               //      return false;
               //   }
               //   return onlyWhiteSpace(HttpUtility.HtmlDecode(txt), true);

               case (char)0xA0:
               case ' ':
               case '\r':
               case '\n':
               case '\t': continue;
            }
//            logger.Log("Non empty char={0:X}", (int)txt[i]);
            return false;
         }
         return true;
      }
      private void processTitleNodes(HtmlNodeCollection titleNodes)
      {
         if (titleNodes == null) return;
         List<HtmlNode> list = toList(titleNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlNode node = list[i];
            addMetaData("title", node.InnerText);
            if (removeTitleNodes) node.Remove();
         }
      }

      private void addMetaData(String key, String value)
      {
         if (String.IsNullOrEmpty(key)) return;
         //if (Properties.ContainsKey(key)) return;
         if (String.IsNullOrEmpty(value)) return;
         //Properties.Add(key, value);
         Properties.Add(new KeyValuePair<string, string>(key, value));
      }


      private static List<HtmlNode> toList(HtmlNodeCollection coll)
      {
         if (coll == null) return new List<HtmlNode>();
         return coll.ToList();
      }
   }
}
