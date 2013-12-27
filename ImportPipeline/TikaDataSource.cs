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

namespace Bitmanager.ImportPipeline
{
   public class TikaDS: Datasource
   {
      private String uriBase;
      private IDatasourceFeeder feeder;
      private int abstractLength, abstractDelta;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         uriBase = node.ReadStr("@tikaurl");
         if (!uriBase.EndsWith("/")) uriBase += "/";
         feeder = ctx.CreateFeeder (node);
         abstractLength = node.OptReadInt("abstract/@maxlength", 256);
         abstractDelta = node.OptReadInt("abstract/@delta", 20);
      }


      private HtmlProcessor loadUrl(String fn)
      {
         Uri uri = new Uri(uriBase + fn);
         using (WebClient client = new WebClient())
         {
            byte[] bytes = client.DownloadData(uri);
            MemoryStream m = new MemoryStream(bytes);
            m.Position = 0;
            HtmlDocument doc = new HtmlDocument();
            doc.Load(m, Encoding.UTF8);
            return new HtmlProcessor (doc);
         }
      }


      private StringDict getAttributes(XmlNode node)
      {
         StringDict ret = new StringDict();
         var coll = node.Attributes;
         for (int i=0; i<coll.Count; i++)
         {
            var att = coll[i];
            if (att.LocalName.Equals ("url", StringComparison.InvariantCultureIgnoreCase)) continue;
            if (att.LocalName.Equals ("baseurl", StringComparison.InvariantCultureIgnoreCase)) continue;
            ret[att.LocalName] = att.Value;
         }
         return ret;
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IDatasourceFeederElement elt)
      {
         StringDict attribs = getAttributes(elt.Context);
         String fileName = elt.ToString();
         sink.HandleValue (ctx, "record/_start", fileName);
         DateTime dtFile = File.GetLastWriteTimeUtc(fileName);
         sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         var htmlProcessor = loadUrl(fileName);

         //Write html properties
         foreach (var kvp in htmlProcessor.Properties)
         {
            sink.HandleValue (ctx, "record/"+kvp.Key, kvp.Value);
         }

         //Add dummy type to recognize the errors
         //if (error)
         //   doc.AddField("content_type", "ConversionError");

         sink.HandleValue (ctx, "record/shortcontent", htmlProcessor.GetAbstract(abstractLength, abstractDelta));
         sink.HandleValue (ctx, "record/content", htmlProcessor.GetNewHtml());

         sink.HandleValue(ctx, "record/_end", fileName);
         sink.HandleValue(ctx, "record", null);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
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
      }

   }


   public class HtmlProcessor
   {
      private String _newHtml;
      public readonly List<KeyValuePair<String,String>> Properties;
      public readonly HtmlDocument Document;
      public HtmlNode HtmlNode {get; private set;}
      public HtmlNode BodyNode { get; private set; }

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
         var headNode = HtmlNode.SelectSingleNode("head");
         if (headNode == null) goto EXIT_RTN; ;

         processMetaNodes(headNode.SelectNodes("meta"));
         processTitleNodes(headNode.SelectNodes("title"));
         removeEmptyTextNodes(headNode.ChildNodes);
      EXIT_RTN:
         Document = doc;
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


      private static readonly char[] TRIMCHARS = { ' ', '\r', '\n' };
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
            node.Remove();
         }
      }
      private void removeEmptyTextNodes(HtmlNodeCollection textNodes)
      {
         if (textNodes == null) return;
         List<HtmlNode> list = toList(textNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlTextNode txtNode = list[i] as HtmlTextNode;
            if (txtNode == null) continue;
            if (onlyWhiteSpace(txtNode))
               txtNode.Remove();
         }
      }

      private static bool onlyWhiteSpace(HtmlTextNode node)
      {
         String txt = node.Text;
         if (String.IsNullOrEmpty(txt)) return true;

         for (int i = 0; i < txt.Length; i++)
         {
            if (txt[i] > ' ') return false;
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
            node.Remove();
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
