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
   public class SortProcessor : PostProcessorBase
   {
      private readonly JComparer sorter;
      private readonly JComparer undupper;
      private readonly UndupActions undupActions;

      private int numAfterSort;

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
               undupActions = new UndupActions(engine, this, actionsNode);
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

      protected override String getStatsLine()
      {
         return String.Format("-- In={0}, out={1}, passed through={2}, sorted={3}.", cnt_received, cnt_added, 0, numAfterSort);
      }


      private void getEnum(AsyncRequestElement ctx)
      {
         int i = (int)ctx.Context;
         ctx.Result = mapper.GetObjectEnumerator (i, true);
      }

      public override int CallNextPostProcessor(PipelineContext ctx)
      {
         ctx.PostProcessor = this;
         ReportStart(ctx);
         if (mapper!=null)
         {
            enumeratePartialAndClose(ctx, mapper.GetObjectEnumerator(0));
         }
         Utils.FreeAndNil(ref mapper);
         ReportEnd(ctx);
         return base.CallNextPostProcessor(ctx);
      }

      private void enumeratePartialAndClose(PipelineContext ctx, MappedObjectEnumerator e)
      {
         if (e == null) return;
         try
         {
            //ctx.ImportLog.Log("enumeratePartial e={0}", e.GetType().Name);
            int cnt = 0;
            if (this.undupper != null)
            {
               List<JObject> list = e.GetAll();
               if (list.Count == 0) goto EXIT_RTN;

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
                  PassThrough(ctx, prev);

                  prevIdx = i;
                  prev = cur;
                  prevKeys = keys;
               }
               if (prevIdx < list.Count)
               {
                  if (undupActions != null)
                     undupActions.ProcessRecords(ctx, list, prevIdx, list.Count - prevIdx);
                  PassThrough(ctx, prev);
               }
            }
            else
            {
               while (true)
               {
                  var obj = e.GetNext();
                  if (obj == null) break;
                  PassThrough(ctx, obj);
               }
            }

EXIT_RTN:
            numAfterSort += cnt;
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
            ++cnt_received;
            mapper.Write(accumulator);
            Clear();
         }
      }

   }
}
