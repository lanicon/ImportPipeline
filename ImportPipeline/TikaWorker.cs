using Bitmanager.Core;
using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Web;

namespace Bitmanager.ImportPipeline
{
   internal class TikaAsyncWorker : AsyncRequestElement
   {
      public readonly TikaDS Parent;
      public readonly StringDict Attribs;
      public readonly FileNameFeederElement FullElt;
      public readonly DateTime LastModifiedUtc;
      public readonly long FileSize;

      public HtmlProcessor HtmlProcessor;
      public String StoredAs;

      private String dbgStoreDir;
      private static int storeNum;

      public TikaAsyncWorker(TikaDS parent, IDatasourceFeederElement elt)
      {
         action = LoadUrl;
         Parent = parent;
         dbgStoreDir = parent.DbgStoreDir;
         Attribs = new StringDict();
         var coll = elt.Context.Attributes;
         for (int i = 0; i < coll.Count; i++)
         {
            var att = coll[i];
            if (att.LocalName.Equals("url", StringComparison.InvariantCultureIgnoreCase)) continue;
            if (att.LocalName.Equals("baseurl", StringComparison.InvariantCultureIgnoreCase)) continue;
            Attribs[att.LocalName] = att.Value;
         }
         FullElt = (FileNameFeederElement)elt;
         FileInfo info = new FileInfo(FullElt.FileName);
         LastModifiedUtc = info.LastWriteTimeUtc;
         FileSize = info.Length;
      }

      public void LoadUrl(AsyncRequestElement elt)
      {
         loadUrl(this.FullElt.FileName);
      }

      private void loadUrl(String fn)
      {
         Uri uri = new Uri(Parent.UriBase + HttpUtility.UrlEncode(fn));
         HttpWebRequest req = (HttpWebRequest)HttpWebRequest.Create(uri);
         req.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
         req.KeepAlive = true;
         HttpWebResponse resp;
         try
         {
            resp = (HttpWebResponse)req.GetResponse();
         }
         catch (WebException we)
         {
            resp = (HttpWebResponse)we.Response;
            Logs.ErrorLog.Log("error: " + we);
            if (resp == null || resp.StatusCode != HttpStatusCode.InternalServerError) throw;
            StreamReader x = new StreamReader(resp.GetResponseStream(), Encoding.UTF8);
            String strResp = x.ReadToEnd();
            Logs.ErrorLog.Log("error={0}", strResp);
            Logs.ErrorLog.Log("url={0}", uri);
            resp.Close();
            throw new BMException(we, strResp);
         }

         HtmlDocument doc;
         using (resp)
         {
            doc = new HtmlDocument();
            using (Stream respStream = resp.GetResponseStream())
            {
               if (dbgStoreDir == null)
                  doc.Load(respStream, Encoding.UTF8);
               else
               {
                  MemoryStream m = new MemoryStream(4096);
                  CopyStream(m, respStream, 4096);
                  storeHtml(fn, m.GetBuffer(), (int)m.Length);
                  m.Position = 0;
                  doc.Load(m, Encoding.UTF8);
               }
            }
         }
         HtmlProcessor = new HtmlProcessor(doc);




         //using (WebClient client = new WebClient())
         //{
         //   byte[] bytes = client.DownloadData(uri);
         //   if (dbgStoreDir != null) storeHtml(fn, bytes);
         //   MemoryStream m = new MemoryStream(bytes);
         //   m.Position = 0;
         //   HtmlDocument doc = new HtmlDocument();
         //   doc.Load(m, Encoding.UTF8);
         //   return new HtmlProcessor (doc);
         //}
      }

      private static void CopyStream(Stream dst, Stream src, int bufferSize)
      {
         byte[] buffer = new byte[bufferSize];
         int count;
         while ((count = src.Read(buffer, 0, buffer.Length)) != 0)
            dst.Write(buffer, 0, count);
      }

      private void storeHtml(string fn, byte[] bytes, int len)
      {
         String name = String.Format("{0}{1}_{2}.html", dbgStoreDir, Path.GetFileName(fn), Interlocked.Increment(ref storeNum));
         Logs.CreateLogger("import", "dbg").Log("store f={0}", name);
         using (var fs = new FileStream(name, FileMode.Create, FileAccess.Write, FileShare.Read))
         {
            fs.Write(bytes, 0, len);
         }
         StoredAs = name;
      }


   }
}
