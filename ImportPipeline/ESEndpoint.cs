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

namespace Bitmanager.ImportPipeline
{
   public class ESEndpoint : Endpoint
   {
      public readonly ESConnection Connection;
      public readonly IndexDefinitionTypes IndexTypes;
      public readonly IndexDefinitions Indexes;
      public readonly IndexDocTypes IndexDocTypes;
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
         CacheSize = node.OptReadInt("@cache", -1);
         MaxParallel = node.OptReadInt("@maxparallel", 0);
         ReadOnly = node.OptReadBool("@readonly", false);
         XmlNode typesNode = node.SelectSingleNode("indextypes");
         if (typesNode != null)
            IndexTypes = new IndexDefinitionTypes(engine.Xml, typesNode);
         Indexes = new IndexDefinitions(IndexTypes, engine.Xml, node.SelectMandatoryNode("indexes"), false);
         IndexDocTypes = new IndexDocTypes(Indexes, node.SelectMandatoryNode("types"));

         String[] arr = node.OptReadStr("waitfor/@status", "green|yellow").SplitStandard();
         WaitForTimeout = node.OptReadInt("waitfor/@timeout", 30);
         WaitForMustExcept = node.OptReadBool("waitfor/@except", false);
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
         ESIndexCmd._CheckIndexFlags flags = ESIndexCmd._CheckIndexFlags.AppendDate;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) != 0) flags |= ESIndexCmd._CheckIndexFlags.ForceCreate;
         Indexes.CreateIndexes(Connection, flags);
         WaitForStatus();
      }

      protected override void Close(PipelineContext ctx)
      {
         if (ReadOnly) return;
         if (!base.logCloseAndCheckForNormalClose(ctx)) return;
         ctx.ImportLog.Log("-- Optional optimize indexes");
         Indexes.OptionalOptimize(Connection);
         ctx.ImportLog.Log("-- Optional rename indexes");
         Indexes.OptionalRename(Connection);
         logCloseDone(ctx);
      }

      public bool WaitForStatus()
      {
         var cmd = Connection.CreateHealthRequest();
         if (WaitFor == AltWaitFor)
            return cmd.WaitForStatus(WaitFor, WaitForTimeout, WaitForMustExcept);
         return cmd.WaitForStatus(WaitFor, AltWaitFor, WaitForTimeout, WaitForMustExcept);
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName)
      {
         if (String.IsNullOrEmpty(dataName))
            return new ESDataEndpoint(this, IndexDocTypes[0]);
         return new ESDataEndpoint(this, IndexDocTypes.GetDocType(dataName, true));
      }
   }


   public class ESDataEndpoint : JsonEndpointBase<ESEndpoint>
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
               asyncQ.Add(new AsyncRequestElement(accumulator, asyncAdd));
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
         JObject accu = ctx.WhatToAdd as JObject;
         if (accu != null)
         {
            Connection.Post(DocType.UrlPart, accu).ThrowIfError();
            return;
         }
         flushCache((List<ESBulkEntry>)ctx.WhatToAdd);
      }

      public void FlushCache()
      {
         if (cache.Count == 0) return;
         if (asyncQ == null)
            flushCache(cache);
         else
            asyncQ.Add(new AsyncRequestElement(cache, asyncAdd));

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
         if (cacheSize > 1)
            cache = new List<ESBulkEntry>(cacheSize);
         else
            cache = null;
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
         if (asyncQ != null) asyncQ.EndInvokeAll();
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


   }

}