using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Net;
using Bitmanager.Importer;


namespace Bitmanager.ImportPipeline
{
   public class UrlFeederElement : FeederElementBase
   {
      public String User { get; protected set; }
      public String Password { get; protected set; }
      public Uri Uri { get; protected set; }
      public UrlFeederElement(XmlNode ctxNode, XmlNode baseNode, String url)
         : base(ctxNode)
      {
         String baseUrl = baseNode == null ? null : baseNode.ReadStr("@baseurl", null);
         Uri = (baseUrl == null) ? new Uri(url) : new Uri(new Uri(baseUrl), url);
         base.Element = Uri;
         User = ctxNode.ReadStr("@user", null);
         Password = ctxNode.ReadStr("@password", null);
      }

      public void OptSetCredentials(PipelineContext ctx, WebRequest req)
      {
         String user = User;
         if (String.IsNullOrEmpty(user)) return;

         String password = Password;
         if (String.IsNullOrEmpty(password) && (ctx.ImportFlags & _ImportFlags.Silent)==0)
         {
            if (!CredentialsHelper.PromptForCredentials(null, Uri.ToString(), ref user, out password)) return;
         }

         CredentialCache credsCache = new CredentialCache();
         NetworkCredential myCred = new NetworkCredential(user, password);
         credsCache.Add(Uri, "Basic", myCred);
         req.Credentials = credsCache;
      }
   }


   public class UrlFeeder : IDatasourceFeeder
   {
      private List<FeederElementBase> urlElements;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         var urls = new List<FeederElementBase>();
         String url = node.ReadStr("@url", null);
         if (url != null)
            urls.Add(new UrlFeederElement (node, node, url));
         else
         {
            XmlNodeList list = node.SelectNodes("url");
            for (int i = 0; i < list.Count; i++)
            {
               String x = list[i].ReadStr(null);
               urls.Add(new UrlFeederElement(list[i], node, x));
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
