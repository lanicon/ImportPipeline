/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
using Bitmanager.ImportPipeline.StreamProviders;


namespace Bitmanager.ImportPipeline
{
   public class ESDatasource : Datasource
   {
      protected RootStreamDirectory streamDirectory;
      private String timeout;
      private String requestBody;
      private int timeoutInMs;
      private int numRecords;
      private int maxParallel;
      private int splitUntil;
      private bool scan;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         streamDirectory = new RootStreamDirectory(ctx, node);
         int size = node.ReadInt("@buffersize", 0);
         numRecords = size > 0 ? size : node.ReadInt("@buffersize", ESRecordEnum.DEF_BUFFER_SIZE);
         timeout = node.ReadStr("@timeout", ESRecordEnum.DEF_TIMEOUT);
         timeoutInMs = Invariant.ToInterval(timeout);
         maxParallel = node.ReadInt("@maxparallel", 1);
         requestBody = node.ReadStr("request", null);
         splitUntil = node.ReadInt("@splituntil", 1);
         scan = node.ReadBool("@scan", true);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in this.streamDirectory.GetProviders(ctx))
         {
            try
            {
               importUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, e.Message + "\r\nUrl=" + elt.Uri + ".");
            }
         }
      }

      class ContextCallback
      {
         IStreamProvider elt;
         PipelineContext ctx;
         ESDatasource ds;
         public ContextCallback(PipelineContext ctx, ESDatasource ds, IStreamProvider elt)
         {
            this.elt = elt;
            this.ctx = ctx;
            this.ds = ds;
         }

         public void OnPrepareRequest(ESConnection conn, HttpWebRequest req)
         {
            WebStreamProvider ufe = elt as WebStreamProvider;
            if (ufe == null) return;
            ufe.PrepareRequest(req);
         }
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         int maxParallel = elt.ContextNode.ReadInt ("@maxparallel", this.maxParallel);
         int splitUntil = elt.ContextNode.ReadInt("@splituntil", this.splitUntil);
         if (splitUntil < 0) splitUntil = int.MaxValue;
         bool scan = elt.ContextNode.ReadBool("@scan", this.scan);

         String url = elt.ToString();
         ctx.SendItemStart(elt);
         String command = elt.ContextNode.ReadStr("@command", null);
         String index = command != null ? null : elt.ContextNode.ReadStr("@index"); //mutual exclusive with command
         String reqBody = elt.ContextNode.ReadStr("request", this.requestBody);
         JObject req = null;
         if (reqBody != null)
            req = JObject.Parse(reqBody);
         ctx.DebugLog.Log("Request scan={1}, body={0}", reqBody, scan);
         try
         {
            Uri uri = new Uri (url);
            ESConnection conn = new ESConnection (url);
            ContextCallback cb = new ContextCallback(ctx, this, elt);
            conn.Timeout = timeoutInMs; //Same timeout as what we send to ES
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
                  ctx.IncrementEmitted();
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
            }
            ctx.SendItemStop();
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }
   }
}
