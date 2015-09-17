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

   public class MapReduceProcessor : PostProcessorBase
   {
      private readonly String directory;
      private readonly JComparer hasher;
      private readonly JComparer sorter;
      private readonly JComparer undupper;
      private readonly UndupActions undupActions;

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

      private MapperWritersBase mapper;

      public MapReduceProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         maxNullIndex = node.ReadInt("@max_null_index", -1);
         fanOut = node.ReadInt("@fan_out", 100);
         if (fanOut <= 0) throw new BMNodeException(node, "Count should be > 0.");

         if (node.ReadInt("write/@maxparallel", 1) > 0)
         {
            bufferSize = node.ReadInt("write/@buffer", 100);
         }
         readMaxParallel = node.ReadInt("read/@maxparallel", 1);

         directory = node.ReadStr("dir/@name", null);
         if (directory != null)
         {
            directory = engine.Xml.CombinePath(directory);
            keepFiles = node.ReadBool("dir/@keepfiles", false);
            compress = node.ReadBool("dir/@compress", true);
         }
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

      public MapReduceProcessor(PipelineContext ctx, MapReduceProcessor other, IDataEndpoint epOrnextProcessor)
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
         if (other.undupActions != null)
            undupActions = other.undupActions.Clone (ctx);
         if (bufferSize > 0)
         {
            buffer = new List<JObject>(bufferSize);
            asyncQ = AsyncRequestQueue.Create(1); 
         }
         ctx.ImportLog.Log ("Postprocessor [{0}]: mapping to {1}. Fan-out={2}.", Name, directory == null ? "<memory>" : directory, fanOut);
      }


      public override IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor)
      {
         return new MapReduceProcessor(ctx, this, epOrnextProcessor);
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
         ctx.PostProcessor = this;
         if (mapper!=null)
         {
            AsyncRequestQueue q = (readMaxParallel == 0 || fanOut <= 1) ? null : AsyncRequestQueue.Create(readMaxParallel);

            MappedObjectEnumerator e;
            if (q == null)
            {
               for (int i = 0; true; i++)
               {
                  e = mapper.GetObjectEnumerator(i);
                  if (e == null) break;
                  ctx.Added += enumeratePartialAndClose(ctx, e);
               }
            }
            else
            {
               //Push enum requests into the Q and process the results
               for (int i = 0; true; i++)
               {
                  var x = q.PushAndOptionalPop(new AsyncRequestElement(i, getEnum));
                  if (x == null) continue;
                  e = (MappedObjectEnumerator)x.Result;
                  if (e == null) break;

                  ctx.Added += enumeratePartialAndClose(ctx, e);
               }

               //Pop all existing from the Q and process them
               while (true)
               {
                  var x = q.Pop();
                  if (x == null) break;
                  e = (MappedObjectEnumerator)x.Result;
                  if (e == null) continue; ;

                  ctx.Added += enumeratePartialAndClose(ctx, e);
               }
            }
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
            if (directory == null)
               mapper = new MemoryBasedMapperWriters(hasher, sorter, fanOut);
            else
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
