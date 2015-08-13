using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class SortProcessor : PostProcessorBase
   {
      private readonly JComparer sorter;
      private readonly JComparer undupper;
      private readonly UndupActions undupActions;

      private int numAfterSort;
      private int numAfterUndup;

      private MapperWritersBase mapper;

      public SortProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("sorter"), "key", false);
         sorter = JComparer.Create(list);

         XmlNode undupNode = node.SelectSingleNode("undupper");
         if (undupNode != null)
         {
            undupper = sorter.Clone(undupNode.ReadStr("@from_sort", null));
            if (undupper == null)
            {
               list = KeyAndType.CreateKeyList(undupNode, "key", true);
               undupper = JComparer.Create(list);
            }

            XmlNode actionsNode = undupNode.SelectSingleNode("actions");
            if (actionsNode != null)
               undupActions = new UndupActions(this, actionsNode);
         }

      }

      public SortProcessor(PipelineContext ctx, SortProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         sorter = other.sorter;
         undupper = other.undupper;
         if (other.undupActions != null)
            undupActions = other.undupActions.Clone (ctx);
      }


      public override IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor)
      {
         return new SortProcessor(ctx, this, epOrnextProcessor);
      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("{0} [type={1}, clone=#{2}, sorter={3}, undup={4}]",
            Name, GetType().Name, InstanceNo, sorter, undupper);
         return sb.ToString();
      }

      private void dumpStats(PipelineContext ctx)
      {
         Logger logger = ctx.ImportLog;
         logger.Log("PostProcessor {0} ended.", this);
         logger.Log("-- In={0}, out={1}, passed through={2}, sorted={3}, after undup={4}.", numAfterSort, numAfterUndup, 0, numAfterSort, numAfterUndup);
      }
      public override void Stop(PipelineContext ctx)
      {
         dumpStats(ctx);
         base.Stop(ctx);
      }



      private void getEnum(AsyncRequestElement ctx)
      {
         int i = (int)ctx.Context;
         ctx.Result = mapper.GetObjectEnumerator (i, true);
      }

      public override void CallNextPostProcessor(PipelineContext ctx)
      {
         ctx.PostProcessor = this;
         if (mapper!=null)
         {
            MappedObjectEnumerator e = mapper.GetObjectEnumerator(0);
            if (e != null)
               ctx.Added += enumeratePartialAndClose(ctx, e);
         }
         Utils.FreeAndNil(ref mapper);
         base.CallNextPostProcessor(ctx);
      }

      private int enumeratePartialAndClose(PipelineContext ctx, MappedObjectEnumerator e)
      {
         try
         {
            //ctx.ImportLog.Log("enumeratePartial e={0}", e.GetType().Name);
            int cnt = 0;
            int exp = 0;
            if (this.undupper != null)
            {
               List<JObject> list = e.GetAll();
               if (list.Count == 0) goto NEXT_PROC;

               cnt = list.Count;
               JObject prev = list[0];
               JToken[] prevKeys = undupper.GetKeys(prev);
               int prevIdx = 0;
               for (int i = 1; i < list.Count; i++)
               {
                  JObject cur = list[i];
                  JToken[] keys = undupper.GetKeys(cur);
                  if (undupper.CompareKeys(prevKeys, keys) == 0) continue;

                  if (undupActions != null)
                     undupActions.ProcessRecords(ctx, list, prevIdx, i - prevIdx);
                  nextEndpoint.SetField(null, prev);
                  nextEndpoint.Add(ctx);
                  ++exp;

                  prevIdx = i;
                  prev = cur;
                  prevKeys = keys;
               }
               if (prevIdx < list.Count)
               {
                  if (undupActions != null)
                     undupActions.ProcessRecords(ctx, list, prevIdx, list.Count - prevIdx);
                  nextEndpoint.SetField(null, prev);
                  nextEndpoint.Add(ctx);
                  ++exp;
               }
            }
            else
            {
               while (true)
               {
                  var obj = e.GetNext();
                  if (obj == null) break;
                  nextEndpoint.SetField(null, obj);
                  nextEndpoint.Add(ctx);
                  ++cnt;
               }
               exp = cnt;
            }

NEXT_PROC:
            numAfterSort += cnt;
            numAfterUndup += exp;
            base.CallNextPostProcessor(ctx);
            return exp;
         }
         finally
         {
            e.Close();
         }
      }

      public override void Add(PipelineContext ctx)
      {
         if (mapper == null)
         {
            String id = String.Format("{0}#{1}", Name, InstanceNo);
            mapper = new MemoryBasedMapperWriters(null, sorter, 1);
         }
         if (accumulator.Count > 0)
         {
            mapper.Write(accumulator);
            Clear();
         }
      }

   }
}
