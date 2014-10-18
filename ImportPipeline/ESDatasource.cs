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
      private bool scan;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         feeder = ctx.CreateFeeder(node, typeof (UrlFeeder));
         int size = node.OptReadInt("@buffersize", 0);
         numRecords = size > 0 ? size : node.OptReadInt("@buffersize", ESRecordEnum.DEF_BUFFER_SIZE);
         timeout = node.OptReadStr("@timeout", ESRecordEnum.DEF_TIMEOUT);
         maxParallel = node.OptReadInt("@maxparallel", 1);
         requestBody = node.OptReadStr("request", null);
         splitUntil = node.OptReadInt("@splituntil", 1);
         scan = node.OptReadBool("@scan", true);
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
         int maxParallel = elt.Context.OptReadInt ("@maxparallel", this.maxParallel);
         int splitUntil = elt.Context.OptReadInt("@splituntil", this.splitUntil);
         if (splitUntil < 0) splitUntil = int.MaxValue;
         bool scan = elt.Context.OptReadBool("@scan", this.scan);

         //StringDict attribs = getAttributes(elt.Context);
         //var fullElt = (FileNameFeederElement)elt;
         String url = elt.ToString();
         sink.HandleValue(ctx, Pipeline.ItemStart, elt);
         String command = elt.Context.OptReadStr("@command", null);
         String index = command != null ? null : elt.Context.ReadStr("@index"); //mutual exclusive with command
         String reqBody = elt.Context.OptReadStr("request", this.requestBody);
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
               foreach (var doc in e)
               {
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
                  sink.HandleValue(ctx, "record", null);
               }
               ctx.ImportLog.Log("Scanned {0} records", e.ScrolledCount);
            }
            sink.HandleValue(ctx, Pipeline.ItemStop, elt);
         }
         catch (Exception e)
         {
            if (!sink.HandleException(ctx, "record", e))
               throw;
         }
      }
   }
}
