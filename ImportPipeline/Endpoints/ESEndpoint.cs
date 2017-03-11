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

using Bitmanager.Elastic;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Core;
using System.Xml;
using System.Collections;
using System.IO;
using Bitmanager.IO;
using Bitmanager.ImportPipeline.Template;
using System.Web;
using Newtonsoft.Json;

namespace Bitmanager.ImportPipeline
{
   public class ESEndpoint : Endpoint
   {
      public readonly ESConnection Connection;
      public readonly ESIndexDefinitions Indexes;
      public readonly int CacheSize;
      public readonly int MaxParallel;

      protected readonly ClusterStatus WaitFor, WaitForAlt;
      protected readonly bool WaitForMustExcept;
      protected readonly int WaitForTimeout;
      public readonly bool NormalCloseOnError;
      public readonly bool ReadOnly;


      public ESEndpoint(ImportEngine engine, XmlNode node)
         : base(engine, node)
      {
         Connection = new ESConnection(node.ReadStr("@url"));
         CacheSize = node.ReadInt("@cache", -1);
         MaxParallel = node.ReadInt("@maxparallel", 0);
         ReadOnly = node.ReadBool("@readonly", false);
         XmlNode root = node.SelectSingleNode("indexes");
         if (root == null) root = node;

         Indexes = new ESIndexDefinitions(engine.Xml, root, _loadConfig);
         if (Indexes.Count == 0)
            throw new BMNodeException(node, "At least 1 index+type is required!");

         WaitFor = ESHealthCmd.SplitRequestedClusterStatus(node.ReadStr("waitfor/@status", "Green | Yellow"), out WaitForAlt);
         WaitForTimeout = node.ReadInt("waitfor/@timeout", 30);
         WaitForMustExcept = node.ReadBool("waitfor/@except", true);
      }

      protected override void Open(PipelineContext ctx)
      {
         base.Open(ctx);
         ESHelper.SetLogging(ctx, Connection);
         ctx.ImportLog.Log("ESEndpoint '{0}' [cache={1}, maxparallel={2}, readonly={3}, url={4}]", Name, CacheSize, MaxParallel, ReadOnly, Connection.BaseUri);
      }

      internal void OpenIndex(PipelineContext ctx, ESIndexDefinition index)
      {
         if (index.IsOpen) return;
         ESHelper.SetLogging(ctx, Connection);
         ESIndexCmd._CheckIndexFlags flags = ESIndexCmd._CheckIndexFlags.AppendDate;
         if (ReadOnly) flags |= ESIndexCmd._CheckIndexFlags.DontCreate;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) != 0) flags |= ESIndexCmd._CheckIndexFlags.ForceCreate;
         index.Create (Connection, flags);
         WaitForStatus();

         var adminEp = this.GetAdminEndpoint(ctx);
         if (adminEp != null)
         {
            int oldCount = ctx.RunAdministrations.Count;
            ctx.RunAdministrations.Merge(adminEp.LoadAdministration(ctx));
            if (ctx.RunAdministrations.Count != oldCount)
               ctx.ImportLog.Log("-- merged {0} run-administrations from endpoint {1}. Now contains {2} runs.", ctx.RunAdministrations.Count - oldCount, this.Name, ctx.RunAdministrations.Count);

         }
      }

      private JObject _loadConfig(ESIndexDefinition index, String fn, out DateTime fileUtcDate)
      {
         if (fn == null)
         {
            fileUtcDate = DateTime.MinValue;
            return null;
         }
         Engine.ImportLog.Log("Loading config via template. fn={0}", fn);
         fileUtcDate = File.GetLastWriteTimeUtc(fn);
         ITemplateEngine template = Engine.TemplateFactory.CreateEngine();
         template.LoadFromFile(fn);
         var rdr = template.ResultAsStream().CreateJsonReader();
         return JObject.Load(rdr);
      }


      protected override void Close(PipelineContext ctx)
      {
         if (ReadOnly) goto CLOSE_BASE; 

         if (!base.logCloseAndCheckForNormalClose(ctx))
         {
            ctx.ImportLog.Log("-- Only flushing indexes");
            Indexes.Flush(Connection);
            goto CLOSE_BASE;
         }
         switch (Indexes.Count)
         {
            case 0: goto CLOSE_BASE;
            case 1: if (Indexes[0].IsOpen) ctx.ImportLog.Log("-- Closing index " + Indexes[0].Name); break;
            default:
               ctx.ImportLog.Log("-- Closing indexes");
               foreach (var x in Indexes)
               {
                  if (!x.IsOpen) continue;
                  ctx.ImportLog.Log("-- -- " + x.Name);
               }
               break;
         }

         ctx.ImportLog.Log("-- Optional optimize indexes");
         try
         {
            Indexes.OptionalOptimize(Connection);
         }
         catch (Exception err)
         {
            ctx.ImportLog.Log(_LogType.ltWarning, "-- Optimize failed: " + err.Message);
            ctx.ErrorLog.Log("-- Optimize failed: " + err.Message);
            ctx.ErrorLog.Log(err);
         }

         ctx.ImportLog.Log("-- Optional rename indexes");
         Indexes.OptionalRename(Connection);
         logCloseDone(ctx);
      CLOSE_BASE:
         base.Close(ctx);
      }

      public bool WaitForStatus()
      {
         var cmd = Connection.CreateHealthRequest();
         return cmd.WaitForStatus(WaitFor, WaitForAlt, WaitForTimeout*1000, WaitForMustExcept);
      }

      protected ESIndexDocType getDocType(String name, bool mustExcept)
      {
         ESIndexDocType ret = null;
         int ix=-1;
         int cnt = 0;
         if (name != null && 0 <= (ix = name.IndexOf('.')))
         {
            ret = getDocType (name.Substring(0, ix), name.Substring (ix+1));
            if (ret == null) goto EXIT_RTN;
            return ret;
         }

         if (String.IsNullOrEmpty(name))
         {
            ESIndexDefinition def = null;
            if (Indexes.Count == 1)
            {
               def = Indexes[0];
               if (def.DocTypes.Count == 1)
                  return def.DocTypes[0];
            }
            goto EXIT_RTN;
         }

         var doctype = getDocType(name, null);
         if (doctype != null) return doctype;

         foreach (var index in Indexes)
         {
            foreach (var dt in index.DocTypes)
            {
               if (String.Equals(name, dt.Name))
               {
                  ret = dt;
                  cnt++;
               }
            }
         }
         if (cnt == 1) return ret;

      EXIT_RTN:
         if (!mustExcept) return null;
         Logger errorLog = Logs.ErrorLog;
         errorLog.Log("Type {0} is not found or is ambiguous. Found cnt={1}. All types:", name, cnt);
         foreach (var index in Indexes)
         {
            errorLog.Log("-- Index {0}:", index.Name);
            foreach (var dt in index.DocTypes)
            {
               if (String.Equals(name, dt.Name)) errorLog.Log("-- -- Type {0}", dt.Name);
            }
         }
         throw new BMException("Cannot find endpoint [{0}]. It is not found or ambiguous.", name);
      }
      protected ESIndexDocType getDocType(String indexName, String typeName)
      {
         foreach (var index in Indexes)
         {
            if (String.Equals(indexName, index.Name))
            {
               if (String.IsNullOrEmpty(typeName))
               {
                  if (index.DocTypes.Count == 1)
                     return index.DocTypes[0];
                  else
                     return null;
               }
               foreach (var doctype in index.DocTypes)
               {
                  if (String.Equals(typeName, doctype.Name)) return doctype;
               }
               return null;
            }
         }
         return null;
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName, bool mustExcept)
      {
         var dt = getDocType(dataName, mustExcept);
         return dt == null ? null : new ESDataEndpoint(this, dt);
      }
      protected override bool CheckDataEndpoint(PipelineContext ctx, string dataName, bool mustExcept)
      {
         var dt = getDocType(dataName, mustExcept);
         return dt != null;
      }

      public override IAdminEndpoint GetAdminEndpoint(PipelineContext ctx)
      {
         var type = getDocType("admin_", false);
         ctx.ImportLog.Log("admin doctype={0}...", type);
         return type==null ? null : new ESDataEndpoint(this, type);
      }
   }


   public class ESDataEndpoint : JsonEndpointBase<ESEndpoint>, IAdminEndpoint, IErrorEndpoint
   {
      public readonly ESConnection Connection;
      public readonly ESIndexDocType DocType;
      private readonly int cacheSize;
      private List<ESBulkEntry> cache;
      private AsyncRequestQueue asyncQ;
      public ESDataEndpoint(ESEndpoint endpoint, ESIndexDocType doctype)
         : base(endpoint)
      {
         this.Connection = endpoint.Connection;
         this.DocType = doctype;
         this.cacheSize = endpoint.CacheSize;
         if (endpoint.MaxParallel > 0)
            asyncQ = AsyncRequestQueue.Create (endpoint.MaxParallel);
      }

      public override void Add(PipelineContext ctx)
      {
         if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0)
         {
            ctx.DebugLog.Log("Add: accumulator.Count={0}", accumulator.Count);
         }
         if (accumulator.Count == 0) return;

         OptLogAdd();

         if (DocType.AutoTimestampFieldName != null)
            accumulator[DocType.AutoTimestampFieldName] = DateTime.UtcNow;

         if (cache == null)
         {
            if (asyncQ == null)
            {
               Connection.Post(DocType.GetUrlForAdd(accumulator), accumulator, _recordSerializer).ThrowIfError();
            }
            else
               asyncQ.PushAndOptionalPop(new AsyncRequestElement(accumulator, asyncAdd));
         }
         else
         {
            cache.Add(new ESBulkEntry(accumulator));
            if (cache.Count >= cacheSize) FlushCache();
         }
         Clear();
      }

      public override void Delete(PipelineContext ctx, String recordKey)
      {
         DocType.DeleteByKey(Connection, recordKey);
      }

      //PW nakijken
      private static Action<JsonWriter, JObject> _recordSerializer=ESBulkSerializeHelper.SerializeDataAndIgnoreMetaProperties;
      private void asyncAdd(AsyncRequestElement ctx)
      {
         JObject accu = ctx.Context as JObject;
         if (accu != null)
         {
            Connection.Post(DocType.GetUrlForAdd(accu), accu, _recordSerializer).ThrowIfError();
            return;
         }
         flushCache((List<ESBulkEntry>)ctx.Context);
      }

      public void FlushCache()
      {
         if (cache.Count == 0) return;
         if (asyncQ == null)
            flushCache(cache);
         else
            asyncQ.PushAndOptionalPop(new AsyncRequestElement(cache, asyncAdd));

         cache = new List<ESBulkEntry>();
      }
      private void flushCache(List<ESBulkEntry> cache)
      {
         //Logs.CreateLogger("import", "esdatendp").Log("Flush {0}", cache.Count);
         if (cache.Count == 0) return;
         Connection.Post(DocType.UrlPart + "/_bulk", cache).ThrowIfError();
      }
      public override void Start(PipelineContext ctx)
      {
         Endpoint.OpenIndex(ctx, this.DocType.Index);
         cache = (cacheSize > 1) ? new List<ESBulkEntry>(cacheSize) : null;
         ctx.ImportLog.Log("start cache=" + cache);
      }

      public override void Stop(PipelineContext ctx)
      {
         ctx.ImportLog.Log("stop cache=" + cache);
         if (cache != null)
         {
            FlushCache();
            cache = null;
         }
         if (asyncQ != null) asyncQ.PopAll();
      }

      public override ExistState Exists(PipelineContext ctx, string key, DateTime? timeStamp)
      {
         ExistState st = DocType.Exists(Connection, key, timeStamp);
         Logs.DebugLog.Log("exist=" + st);
         return st;
         //return doctype.Exists(connection, key, timeStamp);
      }
      public override Object LoadRecord(PipelineContext ctx, String key)
      {
         return DocType.LoadByKey(Connection, key);
      }
      public override void EmitRecord(PipelineContext ctx, String recordKey, String recordField, IDatasourceSink sink, String eventKey, int maxLevel)
      {
         JObject obj = DocType.LoadByKey(Connection, recordKey);
         if (obj == null) return;
         JToken token = (recordField == null) ? obj : obj.GetValue(recordField, StringComparison.InvariantCultureIgnoreCase);
         if (token != null)
            Pipeline.EmitToken(ctx, sink, token, eventKey, maxLevel);
      }


      #region IAdminEndpoint
      public void SaveAdministration(PipelineContext ctx, RunAdministrations admins)
      {
         if (admins == null || admins.Count == 0) return;
         String urlPart = DocType.UrlPart + "/";
         foreach (var a in admins)
         {
            String key = a.DataSource + "_" + a.RunDateUtc.ToString("yyyyMMdd_HHmmss"); 
            Connection.Post(urlPart + key, a.ToJson()).ThrowIfError();
         }
      }

      public RunAdministrations LoadAdministration(PipelineContext ctx)
      {
         JObject cmdObj = JObject.Parse("{ 'sort': [{'adm_date': 'desc'}]}");
         var ret = new RunAdministrations(ctx.ImportEngine.RunAdminSettings);

         try
         {
            String url = ((ctx.ImportFlags & _ImportFlags.FullImport) == 0) ? DocType.UrlPart : DocType.UrlPartForPreviousIndex;
            if (!Connection.Exists (url))
            {
               ctx.ErrorLog.Log("Cannot load previous administration: '{0}' not exists", url);
               return ret;
            }

            var e = new ESRecordEnum(Connection, url, cmdObj, ret.Settings.Capacity, "5m", false);
            foreach (var doc in e)
            {
               RunAdministration ra;
               try
               {
                  ra = new RunAdministration(doc._Source); 
               }
               catch (Exception err)
               {
                  String msg = String.Format("Invalid record in run administration. Skipped.\nRecord={0}.", doc);
                  ctx.ImportLog.Log(_LogType.ltWarning, msg);
                  ctx.ErrorLog.Log(_LogType.ltWarning, msg);
                  ctx.ErrorLog.Log(err);
                  continue;
               }
               ret.Add(ra);
               if (ret.Count >= 500) break;
            }
            return ret.Dump ("loaded");
         }
         catch (Exception err)
         {
            if ((ctx.ImportFlags & _ImportFlags.FullImport) == 0) throw;
            ctx.ErrorLog.Log("Cannot load previous administration:");
            ctx.ErrorLog.Log(err);
            return ret;
         }
      }
      #endregion

      #region IErrorEndpoint
      public void SaveError(PipelineContext ctx, Exception err)
      {
         Object key = null;
         IDictionary dict = err.Data;
         if (dict != null) key = dict["key"];
         if (key == null && ctx.Pipeline != null) key = ctx.Pipeline.GetVariable("key");

         JObject errObj = new JObject();
         errObj["err_key"] = key == null ? String.Empty : key.ToString();
         errObj["err_date"] = DateTime.UtcNow;
         errObj["err_ds"] = ctx.DatasourceAdmin.Name;
         errObj["err_text"] = err.Message;
         errObj["err_stack"] = err.StackTrace;
         Connection.Post(DocType.UrlPart, errObj).ThrowIfError();
      }
      #endregion
   }

}