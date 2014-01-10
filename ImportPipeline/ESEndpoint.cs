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
   public class ESEndPoint: EndPoint
   {
      public readonly ESConnection Connection;
      public readonly IndexDefinitionTypes IndexTypes;
      public readonly IndexDefinitions Indexes;
      public readonly IndexDocTypes IndexDocTypes;
      protected readonly ClusterStatus WaitFor, AltWaitFor;
      protected readonly bool WaitForMustExcept;
      protected readonly int WaitForTimeout;


      public ESEndPoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         Connection = new ESConnection(node.ReadStr("@url"));
         XmlNode typesNode = node.SelectSingleNode("indextypes");
         if (typesNode != null)
            IndexTypes = new IndexDefinitionTypes(engine.Xml, typesNode);
         Indexes = new IndexDefinitions(IndexTypes, engine.Xml, node.SelectMandatoryNode("indexes"), false);
         IndexDocTypes = new IndexDocTypes(Indexes, node.SelectMandatoryNode("types"));

         String[] arr = node.OptReadStr ("waitfor/@status", "green|yellow").SplitStandard();
         WaitForTimeout = node.OptReadInt("waitfor/@timeout", 30);
         WaitForMustExcept = node.OptReadBool("waitfor/@except", false);
         try
         {
            if (arr.Length == 1)
            {
               WaitFor = Invariant.ToEnum<ClusterStatus> (arr[0]);
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
         if ((ctx.Flags & _ImportFlags.ImportFull) != 0) flags |= ESIndexCmd._CheckIndexFlags.ForceCreate;
         Indexes.CreateIndexes(Connection, flags);
         WaitForStatus();
      }
      protected override void Close(PipelineContext ctx, bool isError)
      {
         if (isError) return;
         Indexes.OptionalRename(Connection);
      }

      public bool WaitForStatus()
      {
         var cmd = Connection.CreateHealthRequest();
         if (WaitFor==AltWaitFor)
            return cmd.WaitForStatus(WaitFor, WaitForTimeout, WaitForMustExcept);
         return cmd.WaitForStatus(WaitFor, AltWaitFor, WaitForTimeout, WaitForMustExcept);
      }

      protected override IDataEndpoint CreateDataEndPoint(string namePart2)
      {
         if (String.IsNullOrEmpty(namePart2))
            return new ESDataEndpoint(this, IndexDocTypes[0]);
         return new ESDataEndpoint(this, IndexDocTypes.GetDocType(namePart2, true));
      }
   }


   public class ESDataEndpoint : JsonEndpointBase<ESEndPoint>
   {
      private readonly ESConnection connection;
      private readonly IndexDocType doctype;

      public ESDataEndpoint(ESEndPoint endpoint, IndexDocType doctype)
         : base(endpoint)
      {
         this.connection = endpoint.Connection;
         this.doctype = doctype;
      }

      public override void Opened(PipelineContext ctx)
      {
      }

      public override void Add(PipelineContext ctx)
      {
         OptLogAdd();
         connection.Post(doctype.UrlPart, accumulator).ThrowIfError();
         Clear();
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
      public override void EmitRecord(PipelineContext ctx, String key, IDatasourceSink sink, String eventKey, int maxLevel)
      {
         JObject obj = doctype.LoadByKey(connection, key);
         Pipeline.EmitToken(ctx, sink, obj, eventKey, maxLevel);
      }

   }

}
