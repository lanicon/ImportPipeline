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
   public class ESEndPoint : EndPoint
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


      public ESEndPoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         Connection = new ESConnection(node.ReadStr("@url"));
         CacheSize = node.OptReadInt("@cache", -1);
         MaxParallel = node.OptReadInt("@maxparallel", 0);
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
         ESIndexCmd._CheckIndexFlags flags = ESIndexCmd._CheckIndexFlags.AppendDate;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) != 0) flags |= ESIndexCmd._CheckIndexFlags.ForceCreate;
         Indexes.CreateIndexes(Connection, flags);
         WaitForStatus();
      }
      protected override void Close(PipelineContext ctx, bool isError)
      {
         ctx.ImportLog.Log("Closing endpoint '{0}', error={1}, flags={2}", Name, isError, ctx.ImportFlags);
         if (isError || (ctx.ImportFlags & _ImportFlags.DoNotRename) != 0) return;
         ctx.ImportLog.Log("-- Rename indexes");
         Indexes.OptionalRename(Connection);
      }

      public bool WaitForStatus()
      {
         var cmd = Connection.CreateHealthRequest();
         if (WaitFor == AltWaitFor)
            return cmd.WaitForStatus(WaitFor, WaitForTimeout, WaitForMustExcept);
         return cmd.WaitForStatus(WaitFor, AltWaitFor, WaitForTimeout, WaitForMustExcept);
      }

      protected override IDataEndpoint CreateDataEndPoint(PipelineContext ctx, string dataName)
      {
         if (String.IsNullOrEmpty(dataName))
            return new ESDataEndpoint(this, IndexDocTypes[0]);
         return new ESDataEndpoint(this, IndexDocTypes.GetDocType(dataName, true));
      }
   }


   public class ESDataEndpoint : JsonEndpointBase<ESEndPoint>
   {
      private readonly ESConnection connection;
      private readonly IndexDocType doctype;
      private readonly int cacheSize;
      private List<ESBulkEntry> cache;
      private AsyncEndpointRequestQueue asyncQ;
      public ESDataEndpoint(ESEndPoint endpoint, IndexDocType doctype)
         : base(endpoint)
      {
         this.connection = endpoint.Connection;
         this.doctype = doctype;
         this.cacheSize = endpoint.CacheSize;
         if (endpoint.MaxParallel > 0)
            asyncQ = AsyncEndpointRequestQueue.Create (endpoint.MaxParallel);
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
               connection.Post(doctype.UrlPart, accumulator).ThrowIfError();
            else
               asyncQ.Add(new AsyncEndpointRequest(accumulator, asyncAdd), true);
         }
         else
         {
            cache.Add(new ESBulkEntry(accumulator));
            if (cache.Count >= cacheSize) flushCache();
         }
         Clear();
      }

      private void asyncAdd(AsyncEndpointRequest ctx)
      {
         JObject accu = ctx.WhatToAdd as JObject;
         if (accu != null)
         {
            connection.Post(doctype.UrlPart, accu).ThrowIfError();
            return;
         }
         flushCache((List<ESBulkEntry>)ctx.WhatToAdd);
      }

      private void flushCache()
      {
         if (cache.Count == 0) return;
         if (asyncQ == null)
            flushCache(cache);
         else
            asyncQ.Add(new AsyncEndpointRequest(cache, asyncAdd), true);

         cache = new List<ESBulkEntry>();
      }
      private void flushCache(List<ESBulkEntry> cache)
      {
         //Logs.CreateLogger("import", "esdatendp").Log("Flush {0}", cache.Count);
         if (cache.Count == 0) return;
         connection.Post(doctype.UrlPart + "/_bulk", cache).ThrowIfError();
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
            flushCache();
            cache = null;
         }
         if (asyncQ != null) asyncQ.EndInvokeAll();
      }

      public override ExistState Exists(PipelineContext ctx, string key, DateTime? timeStamp)
      {
         ExistState st = doctype.Exists(connection, key, timeStamp);
         Logs.DebugLog.Log("exist=" + st);
         return st;
         //return doctype.Exists(connection, key, timeStamp);
      }
      public override Object LoadRecord(PipelineContext ctx, String key)
      {
         return doctype.LoadByKey(connection, key);
      }
      public override void EmitRecord(PipelineContext ctx, String recordKey, String recordField, IDatasourceSink sink, String eventKey, int maxLevel)
      {
         JObject obj = doctype.LoadByKey(connection, recordKey);
         if (obj == null) return;
         JToken token = (recordField == null) ? obj : obj.GetValue(recordField, StringComparison.InvariantCultureIgnoreCase);
         if (token != null)
            Pipeline.EmitToken(ctx, sink, token, eventKey, maxLevel);
      }


   }

}