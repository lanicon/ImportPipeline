using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;


namespace Bitmanager.ImportPipeline
{

   public class UrlProvider : IDatasourceContentProvider
   {
      private List<Uri> urls;
      private static Uri createUri(Uri baseUri, String url)
      {
         return baseUri == null ? new Uri(url) : new Uri(baseUri, url);
      }
      public void Init(PipelineContext ctx, XmlNode node)
      {
         List<Uri> urls = new List<Uri>();
         String baseUrl = node.OptReadStr("@baseurl", null);
         Uri baseUri = baseUrl == null ? null : new Uri(baseUrl);
         String url = node.OptReadStr("@url", null);
         if (url != null)
            urls.Add(createUri(baseUri, url));
         {
            XmlNodeList list = node.SelectNodes("url");
            for (int i = 0; i < list.Count; i++)
            {
               String x = list[i].ReadStr(null);
               urls.Add(createUri(baseUri, x));
            }
            if (urls.Count == 0) node.ReadStr("@url"); //Raise exception
         }
         this.urls = urls;
      }
      public IEnumerator<object> GetEnumerator()
      {
         for (int i=0; i<urls.Count; i++)
            yield return urls[i];
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }
   }
}
