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
//   <postprocessor name=abc type=fileBaseMapper>
//   <dir name=temp keepfiles=false compress=true />
//   <hasher>
//      <key expr=boe type=int />
//      <key expr=boe type=int />
//   </hasher>
//</postprocessor>

   public class FileBasedMapperProcessor : PostProcessorBase
   {
      private readonly String directory;
      private readonly JComparer hasher;
      private readonly JComparer sorter;
      private readonly JComparer undupper;

      private List<JObject> buffer;
      private AsyncRequestQueue asyncQ;

      private readonly int fanOut;
      private readonly int maxNullIndex;
      private readonly int bufferSize;
      private readonly int readMaxParallel;
      private readonly bool keepFiles, compress;

      private int numPassThrough;
      private int numAfterSort;
      private int numAfterUndup;

      private FileBasedMapperWriters mapper;

      public FileBasedMapperProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         if (node.ReadInt("write/@maxparallel", 1) > 0)
         {
            bufferSize = node.ReadInt("write/@buffer", 100);
         }
         readMaxParallel = node.ReadInt("read/@maxparallel", 1);

         directory = engine.Xml.CombinePath(node.ReadStr("dir/@name"));
         keepFiles = node.ReadBool("dir/@keepfiles", false);
         compress = node.ReadBool("dir/@compress", true);
         maxNullIndex = node.ReadInt("dir/@max_null_index", -1);
         fanOut = node.ReadInt("dir/@fan_out", 100);
         if (fanOut <= 0) throw new BMNodeException(node, "Count should be > 0.");

         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("sorter"), "key", false);
         sorter = JComparer.Create(list);

         XmlNode hashNode = node.SelectSingleNode("hasher");
         if (hashNode == null)
            hasher = sorter;
         else
         {
            hasher = sorter.Clone (hashNode.ReadStr("@from_sort", null));
            if (hasher==null)
            {
               list = KeyAndType.CreateKeyList(hashNode, "key", true);
               hasher = JComparer.Create(list);
            }
         }

         XmlNode undupNode = node.SelectSingleNode("undup");
         if (undupNode != null)
         {
            undupper = sorter.Clone(undupNode.ReadStr("@from_sort", null));
            if (undupper == null)
            {
               list = KeyAndType.CreateKeyList(undupNode, "key", true);
               undupper = JComparer.Create(list);
            }
         }

      }

      public FileBasedMapperProcessor(FileBasedMapperProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         directory = other.directory;
         hasher = other.hasher;
         sorter = other.sorter;
         undupper = other.undupper;
         keepFiles = other.keepFiles;
         fanOut = other.fanOut;
         compress = other.compress;
         maxNullIndex = other.maxNullIndex;
         bufferSize = other.bufferSize;
         readMaxParallel = other.readMaxParallel;
         if (bufferSize > 0)
         {
            buffer = new List<JObject>(bufferSize);
            asyncQ = AsyncRequestQueue.Create(1); 
         }
      }


      public override IPostProcessor Clone(IDataEndpoint epOrnextProcessor)
      {
         return new FileBasedMapperProcessor(this, epOrnextProcessor);
      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("{0} [type={1}, clone=#{7}, max_null_index={2}, fan_out={3}, sorter={4}, hasher={5}, undup={6}]",
            Name, GetType().Name, maxNullIndex, fanOut, sorter, hasher, undupper, InstanceNo);
         return sb.ToString();
      }

      private void dumpStats(PipelineContext ctx)
      {
         Logger logger = ctx.ImportLog;
         logger.Log("PostProcessor {0} ended.", this);
         logger.Log("-- In={0}, out={1}, passed through={2}, sorted={3}, after undup={4}.", numPassThrough + numAfterSort, numPassThrough + numAfterUndup, numPassThrough, numAfterSort, numAfterUndup);
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
         if (mapper!=null)
         {
            AsyncRequestQueue q = (readMaxParallel == 0 || fanOut <= 1) ? null : AsyncRequestQueue.Create(readMaxParallel);
            int total = 0;
            int parts = 0;

            ObjectEnumerator e;
            if (q == null)
            {
               for (int i = 0; true; i++)
               {
                  e = mapper.GetObjectEnumerator(i);
                  if (e == null) break;
                  total += enumeratePartialAndClose(ctx, e);
                  parts++;
               }
            }
            else
            {
               //Push enum requests into the Q and process the results
               for (int i = 0; true; i++)
               {
                  var x = q.PushAndOptionalPop(new AsyncRequestElement(i, getEnum));
                  if (x == null) continue;
                  e = (ObjectEnumerator)x.Result;
                  if (e == null) break;

                  total += enumeratePartialAndClose(ctx, e);
                  parts++;
               }

               //Pop all existing from the Q and process them
               while (true)
               {
                  var x = q.Pop();
                  if (x == null) break;
                  e = (ObjectEnumerator)x.Result;
                  if (e == null) continue; ;

                  total += enumeratePartialAndClose(ctx, e);
                  parts++;
               }
            }
         }
         Utils.FreeAndNil(ref mapper);
         base.CallNextPostProcessor(ctx);
      }

      private int enumeratePartialAndClose(PipelineContext ctx, ObjectEnumerator e)
      {
         try
         {
            //ctx.ImportLog.Log("enumeratePartial e={0}", e.GetType().Name);
            int cnt = 0;
            while (true)
            {
               var obj = e.GetNext();
               if (obj == null) break;
               nextEndpoint.SetField(null, obj);
               nextEndpoint.Add(ctx);
               ++cnt;
            }
            base.CallNextPostProcessor(ctx);
            numAfterSort += cnt;
            numAfterUndup += cnt;
            return cnt;
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
            mapper = new FileBasedMapperWriters(hasher, sorter, directory, id, fanOut, compress, keepFiles);
         }
         if (accumulator.Count > 0)
         {
            if (!mapper.OptWrite(accumulator, maxNullIndex))
            {
               //Just passthrough to the next endpoint if this record had a failing hash-value
               ++numPassThrough;
               nextEndpoint.SetField(null, accumulator);
               nextEndpoint.Add(ctx);
            }
            Clear();
         }
      }

      private void asyncAdd(AsyncRequestElement ctx)
      {
         List<JObject> list = ctx.Context as List<JObject>;
         if (list != null)
         {
            foreach (var obj in list)
            {
               if (!mapper.OptWrite(accumulator, maxNullIndex))
               {
                  //Just passthrough to the next endpoint if this record had a failing hash-value
                  nextEndpoint.SetField(null, accumulator);
                  nextEndpoint.Add(null);
               }

            }
         }
      }

      public void FlushCache()
      {
         if (buffer.Count == 0) return;
         asyncQ.PushAndOptionalPop(new AsyncRequestElement(buffer, asyncAdd));

         buffer = new List<JObject>();
      }

   }
}
