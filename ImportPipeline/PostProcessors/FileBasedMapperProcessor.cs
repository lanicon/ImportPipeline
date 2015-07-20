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
      private readonly JComparer comparer;

      private List<JObject> buffer;
      private AsyncRequestQueue asyncQ;

      private readonly int numFiles;
      private readonly int maxNullIndex;
      private readonly int bufferSize;
      private readonly int readMaxParallel;
      private readonly bool keepFiles, compress;

      private FileBasedMapperWriters mapper = null;

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
         numFiles = node.ReadInt("dir/@count", 100);
         if (numFiles <= 0) throw new BMNodeException(node, "Count should be > 0.");

         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("hasher"), "key", true);
         hasher = JComparer.Create(list);

         XmlNode sortNode = node.SelectSingleNode("sorter");
         if (sortNode == null)
            comparer = hasher;
         else
         {
            comparer = hasher.Clone (sortNode.ReadStr("@from_hash", null));
            if (comparer==null)
            {
               list = KeyAndType.CreateKeyList(sortNode, "key", true);
               comparer = JComparer.Create(list);
            }
         }
      }

      public FileBasedMapperProcessor(FileBasedMapperProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         directory = other.directory;
         hasher = other.hasher;
         comparer = other.comparer;
         keepFiles = other.keepFiles;
         numFiles = other.numFiles;
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

      private void getEnum(AsyncRequestElement ctx)
      {
         int i = (int)ctx.Context;
         ctx.Result = mapper.GetObjectEnumerator (i, true);
      }

      public override void CallNextPostProcessor(PipelineContext ctx)
      {
         if (mapper!=null)
         {
            ctx.ImportLog.Log("CallNextPostProcessor mp={0}, numF={1}, sort={2}", readMaxParallel, numFiles, comparer);
            AsyncRequestQueue q = (readMaxParallel == 0 || numFiles <= 1) ? null : AsyncRequestQueue.Create(readMaxParallel);
            int total = 0;
            int parts = 0;

            FileBasedMapperEnumerator e;
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
               for (int i = 0; true; i++)
               {
                  var x = q.PushAndOptionalPop(new AsyncRequestElement(i, getEnum));
                  if (x == null) continue;
                  e = (FileBasedMapperEnumerator)x.Result;
                  if (e == null) break;

                  total += enumeratePartialAndClose(ctx, e);
                  parts++;
               }

               while (true)
               {
                  var x = q.Pop();
                  if (x == null) break;
                  e = (FileBasedMapperEnumerator)x.Result;
                  if (e == null) continue; ;

                  total += enumeratePartialAndClose(ctx, e);
                  parts++;
               }
            }

            ctx.ImportLog.Log ("Reduce phase emitted {0} recs, parts={1}, q={2}", total, parts, q);


         }

         base.CallNextPostProcessor(ctx);
      }

      private int enumeratePartialAndClose(PipelineContext ctx, FileBasedMapperEnumerator e)
      {
         try
         {
            ctx.ImportLog.Log("enumeratePartial e={0}", e.GetType().Name);
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
            return cnt;
         }
         finally
         {
            e.Close();
         }
      }

      public override void Add(PipelineContext ctx)
      {
         if (mapper == null) mapper = new FileBasedMapperWriters(hasher, comparer, directory, Name, numFiles, compress, keepFiles);
         if (accumulator.Count > 0)
         {
            if (!mapper.OptWrite(accumulator, maxNullIndex))
            {
               //Just passthrough to the next endpoint if this record had a failing hash-value
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
