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

namespace Bitmanager.ImportPipeline
{
   public class ESEndpoint : Endpoint
   {
      public readonly ESConnection Connection;
      public readonly IndexDefinitionTypes IndexTypes;
      public readonly IndexDefinitions Indexes;
      //public readonly IndexDocTypes IndexDocTypes;
      public readonly int CacheSize;
      public readonly int MaxParallel;

      protected readonly ClusterStatus WaitFor, AltWaitFor;
      protected readonly bool WaitForMustExcept;
      protected readonly int WaitForTimeout;
      public readonly bool NormalCloseOnError;
      public readonly bool ReadOnly;


      public ESEndpoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         Connection = new ESConnection(node.ReadStr("@url"));
         CacheSize = node.ReadInt("@cache", -1);
         MaxParallel = node.ReadInt("@maxparallel", 0);
         ReadOnly = node.ReadBool("@readonly", false);
         XmlNode typesNode = node.SelectSingleNode("indextypes");
         if (typesNode != null)
            IndexTypes = new IndexDefinitionTypes(engine.Xml, typesNode);
         XmlNode root = node.SelectSingleNode("indexes");
         if (root == null) root = node;

         Indexes = new IndexDefinitions(IndexTypes, engine.Xml, root, false);
         if (Indexes.Count == 0)
            throw new BMNodeException(node, "At least 1 index+type is required!");

         String[] arr = node.ReadStr("waitfor/@status", "yellow").SplitStandard();
         WaitForTimeout = node.ReadInt("waitfor/@timeout", 30);
         WaitForMustExcept = node.ReadBool("waitfor/@except", false);
         try
         {
            if (arr.Length == 1)
            {
               WaitFor = Invariant.ToEnum<ClusterStatus>(arr[0]);
               AltWaitFor = WaitFor;
            }
            else
            {
               WaitFor = Invariant.ToEnum<ClusterStatus>(arr[0]);
               AltWaitFor = Invariant.ToEnum<ClusterStatus>(arr[1]);
            }
         }
         catch (Exception err)
         {
            throw new BMNodeException(node, err);
         }
      }

      protected override void Open(PipelineContext ctx)
      {
         if (ReadOnly) return;
         base.Open(ctx);
         ctx.ImportLog.Log("ESEndpoint '{0}' [cache={1}, maxparallel={2}, readonly={3}, url={4}]", Name, CacheSize, MaxParallel, ReadOnly, Connection.BaseUri);
      }

      internal void OpenIndex(PipelineContext ctx, IndexDefinition index)
      {
         if (index.IsOpen) return;
         ESIndexCmd._CheckIndexFlags flags = ESIndexCmd._CheckIndexFlags.AppendDate;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) != 0) flags |= ESIndexCmd._CheckIndexFlags.ForceCreate;
         index.Create (Connection, flags);
         WaitForStatus();
      }

      protected override void Close(PipelineContext ctx)
      {
         if (ReadOnly) return;

         if (!base.logCloseAndCheckForNormalClose(ctx)) goto CLOSE_BASE;
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
         if (WaitFor == AltWaitFor)
            return cmd.WaitForStatus(WaitFor, WaitForTimeout, WaitForMustExcept);
         return cmd.WaitForStatus(WaitFor, AltWaitFor, WaitForTimeout, WaitForMustExcept);
      }

      protected IndexDocType getDocType(String name, bool mustExcept)
      {
         IndexDocType ret = null;
         int ix=-1;
         if (name != null && 0 <= (ix = name.IndexOf('.')))
         {
            ret = getDocType (name.Substring(0, ix), name.Substring (ix+1));
            if (ret == null) goto EXIT_RTN;
            return ret;
         }

         if (String.IsNullOrEmpty(name))
         {
            IndexDefinition def = null;
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

         int cnt = 0;
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
         throw new BMException("Cannot find endpoint [{0}]. It is not found or it is ambiguous.", name);
      }
      protected IndexDocType getDocType(String indexName, String typeName)
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
         return type==null ? null : new ESDataEndpoint(this, type);
      }
   }


   public class ESDataEndpoint : JsonEndpointBase<ESEndpoint>, IAdminEndpoint, IErrorEndpoint
   {
      public readonly ESConnection Connection;
      public readonly IndexDocType DocType;
      private readonly int cacheSize;
      private List<ESBulkEntry> cache;
      private AsyncRequestQueue asyncQ;
      public ESDataEndpoint(ESEndpoint endpoint, IndexDocType doctype)
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
         if (cache == null)
         {
            if (asyncQ == null)
               Connection.Post(DocType.UrlPart, accumulator).ThrowIfError();
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

      private void asyncAdd(AsyncRequestElement ctx)
      {
         JObject accu = ctx.Context as JObject;
         if (accu != null)
         {
            Connection.Post(DocType.UrlPart, accu).ThrowIfError();
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
      public void SaveAdministration(PipelineContext ctx, List<RunAdministration> admins)
      {
         if (admins == null || admins.Count == 0) return;
         String urlPart = DocType.UrlPart + "/";
         foreach (var a in admins)
         {
            String key = a.DataSource + "_" + a.RunDateUtc.ToString("yyyyMMdd_HHmmss"); 
            Connection.Post(urlPart + key, a.ToJson()).ThrowIfError();
         }
      }

      public List<RunAdministration> LoadAdministration(PipelineContext ctx)
      {
         List<RunAdministration> ret = new List<RunAdministration>();
         var e = Connection.CreateEnumerator(this.DocType.UrlPart);
         foreach (var doc in e)
         {
            ret.Add(new RunAdministration(doc));
         }
         return ret;
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