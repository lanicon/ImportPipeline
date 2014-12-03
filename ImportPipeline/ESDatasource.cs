using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using Bitmanager.Elastic;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System.Xml;
using System.Net;


namespace Bitmanager.ImportPipeline
{
   public class ESDatasource : Datasource
   {
      private IDatasourceFeeder feeder;
      private String timeout;
      private String requestBody;
      private int numRecords;
      private int maxParallel;
      private int splitUntil;
      private int maxRecords;
      private bool scan;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         feeder = ctx.CreateFeeder(node, typeof (UrlFeeder));
         int size = node.ReadInt("@buffersize", 0);
         numRecords = size > 0 ? size : node.ReadInt("@buffersize", ESRecordEnum.DEF_BUFFER_SIZE);
         timeout = node.ReadStr("@timeout", ESRecordEnum.DEF_TIMEOUT);
         maxParallel = node.ReadInt("@maxparallel", 1);
         requestBody = node.ReadStr("request", null);
         splitUntil = node.ReadInt("@splituntil", 1);
         maxRecords = node.ReadInt("@maxrecords", -1);
         scan = node.ReadBool("@scan", true);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in feeder.GetElements(ctx))
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

      class ContextCallback
      {
         IDatasourceFeederElement elt;
         PipelineContext ctx;
         ESDatasource ds;
         public ContextCallback(PipelineContext ctx, ESDatasource ds, IDatasourceFeederElement elt)
         {
            this.elt = elt;
            this.ctx = ctx;
            this.ds = ds;
         }

         public void OnPrepareRequest(ESConnection conn, HttpWebRequest req)
         {
            UrlFeederElement ufe = elt as UrlFeederElement;
            if (ufe == null) return;

            ufe.OptSetCredentials(ctx, req);
         }
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IDatasourceFeederElement elt)
      {
         int maxParallel = elt.Context.ReadInt ("@maxparallel", this.maxParallel);
         int splitUntil = elt.Context.ReadInt("@splituntil", this.splitUntil);
         if (splitUntil < 0) splitUntil = int.MaxValue;
         bool scan = elt.Context.ReadBool("@scan", this.scan);

         //StringDict attribs = getAttributes(elt.Context);
         //var fullElt = (FileNameFeederElement)elt;
         String url = elt.ToString();
         sink.HandleValue(ctx, Pipeline.ItemStart, elt);
         String command = elt.Context.ReadStr("@command", null);
         String index = command != null ? null : elt.Context.ReadStr("@index"); //mutual exclusive with command
         String reqBody = elt.Context.ReadStr("request", this.requestBody);
         JObject req = null;
         if (reqBody != null)
            req = JObject.Parse(reqBody);
         ctx.DebugLog.Log("Request scan={1}, body={0}", reqBody, scan);
         try
         {
            Uri uri = new Uri (url);
            ESConnection conn = new ESConnection (url);
            ContextCallback cb = new ContextCallback(ctx, this, elt);
            conn.OnPrepareRequest = cb.OnPrepareRequest;   
            if (command != null)
            {
               var resp = conn.SendCmd("POST", command, reqBody);
               resp.ThrowIfError();
               Pipeline.EmitToken(ctx, sink, resp.JObject, "response", splitUntil);
            }
            else
            {
               ESRecordEnum e = new ESRecordEnum(conn, index, req, numRecords, timeout, scan);
               if (maxParallel > 0) e.Async = true;
               ctx.ImportLog.Log("Starting scan of {0} records. Index={1}, connection={2}, async={3}, buffersize={4} requestbody={5}, splituntil={6}, scan={7}.", e.Count, index, url, e.Async, numRecords, req != null, splitUntil, scan);
               int cnt = 0;
               foreach (var doc in e)
               {
                  if (maxRecords > 0 && cnt >= maxRecords) break;
                  ++cnt;
                  sink.HandleValue(ctx, "record/_sort", doc.Sort);
                  sink.HandleValue(ctx, "record/_type", doc.Type);
                  foreach (var kvp in doc)
                  {
                     String pfx = "record/" + kvp.Key;
                     if (splitUntil <= 1)
                     {
                        sink.HandleValue(ctx, pfx, kvp.Value);
                        continue;
                     }
                     Pipeline.EmitToken(ctx, sink, kvp.Value, pfx, splitUntil - 1);
                  }
                  sink.HandleValue(ctx, "record", doc);
               }
               ctx.ImportLog.Log("Scanned {0} records", e.ScrolledCount);
            }
            sink.HandleValue(ctx, Pipeline.ItemStop, elt);
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }
   }
}
