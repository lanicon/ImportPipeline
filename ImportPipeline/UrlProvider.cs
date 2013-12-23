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

   public class UrlFeeder : IDatasourceFeeder
   {
      private List<FeederElementBase> urlElements;
      private static FeederElementBase createUri(XmlNode ctx, Uri baseUri, String url)
      {
         return new FeederElementBase (ctx, baseUri == null ? new Uri(url) : new Uri(baseUri, url));
      }
      public void Init(PipelineContext ctx, XmlNode node)
      {
         var urls = new List<FeederElementBase>();
         String baseUrl = node.OptReadStr("@baseurl", null);
         Uri baseUri = baseUrl == null ? null : new Uri(baseUrl);
         String url = node.OptReadStr("@url", null);
         if (url != null)
            urls.Add(createUri(node, baseUri, url));
         else
         {
            XmlNodeList list = node.SelectNodes("url");
            for (int i = 0; i < list.Count; i++)
            {
               String x = list[i].ReadStr(null);
               urls.Add(createUri(list[i], baseUri, x));
            }
            if (urls.Count == 0) node.ReadStr("@url"); //Raise exception
         }
         this.urlElements = urls;
      }
      public IEnumerator<IDatasourceFeederElement> GetEnumerator()
      {
         for (int i=0; i<urlElements.Count; i++)
            yield return urlElements[i];
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }
   }
}
