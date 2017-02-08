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
   public class DelayedScriptHolder
   {
      public readonly XmlNode Node;
      public readonly String ScriptName;
      public readonly bool IsExpression;
      private PipelineAction.ScriptDelegate scriptDelegate;

      public DelayedScriptHolder (ImportEngine engine, XmlNode node, String name)
      {
         this.Node = node;
         String body = null;
         if (node != null)
            ScriptName = ImportEngine.ReadScriptNameOrBody(node, "@script", out body);

         if (body != null)
         {
            ScriptName = ScriptExpressionHolder.GenerateScriptName("postprocessor_", name, node);
            IsExpression = true;
            engine.ScriptExpressions.AddExpression(ScriptName, body);
         }
      }

      public Object Execute (PipelineContext ctx, Object value)
      {
         if (scriptDelegate == null) 
            scriptDelegate = (ctx.Pipeline.CreateScriptDelegate<PipelineAction.ScriptDelegate>(ScriptName, Node));
         return scriptDelegate(ctx, value);
      }
      public PipelineAction.ScriptDelegate GetDelegate (PipelineContext ctx)
      {
         if (scriptDelegate != null) return scriptDelegate; 
         return scriptDelegate = (ctx.Pipeline.CreateScriptDelegate<PipelineAction.ScriptDelegate>(ScriptName, Node));

      }
   }

   public class SortProcessor : PostProcessorBase
   {
      public readonly JComparer Sorter;
      public readonly JComparer Undupper;
      private readonly UndupActions undupActions;

      private DelayedScriptHolder beforeSort, afterSort; 

      private int numAfterSort;

      private MapperWritersBase mapper;

      public SortProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("sorter"), "key", false);
         Sorter = JComparer.Create(list);

         //Interpret undupper
         XmlNode undupNode = node.SelectSingleNode("undupper");
         if (undupNode != null)
         {
            Undupper = Sorter.Clone(undupNode.ReadStr("@from_sort", null));
            if (Undupper == null)
            {
               list = KeyAndType.CreateKeyList(undupNode, "key", true);
               Undupper = JComparer.Create(list);
            }

            XmlNode actionsNode = undupNode.SelectSingleNode("actions");
            if (actionsNode != null)
               undupActions = new UndupActions(engine, this, actionsNode);
         }

         //Interpret sort scripts
         beforeSort = new DelayedScriptHolder(engine, node.SelectSingleNode("beforesort"), Name);
         if (beforeSort.ScriptName == null) beforeSort = null;
         afterSort = new DelayedScriptHolder(engine, node.SelectSingleNode("aftersort"), Name);
         if (afterSort.ScriptName == null) afterSort = null;
      }

      public SortProcessor(PipelineContext ctx, SortProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         Sorter = other.Sorter;
         Undupper = other.Undupper;
         if (other.undupActions != null)
            undupActions = other.undupActions.Clone (ctx);

         afterSort = other.afterSort;
         beforeSort = other.beforeSort;
      }


      public override IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor)
      {
         return new SortProcessor(ctx, this, epOrnextProcessor);
      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("{0} [type={1}, clone=#{2}, sorter={3}, undup={4}]",
            Name, GetType().Name, InstanceNo, Sorter, Undupper);
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
            List<JObject> list = e.GetAll();
            int cnt = list.Count;
            if (cnt == 0) goto EXIT_RTN;
            if (beforeSort != null)
               list = (List<JObject>) beforeSort.Execute(ctx, list);
            list.Sort(this.Sorter);

            if (this.Undupper == null && this.afterSort == null) goto EXPORT_ALL;

            if (afterSort != null)
               list = (List<JObject>)afterSort.Execute(ctx, list);

            if (this.Undupper == null) goto EXPORT_ALL;

            JObject prev = list[0];
            JToken[] prevKeys = Undupper.GetKeys(prev);
            int prevIdx = 0;
            for (int i = 1; i < list.Count; i++)
            {
               JObject cur = list[i];
               JToken[] keys = Undupper.GetKeys(cur);
               if (Undupper.CompareKeys(prevKeys, keys) == 0) continue;

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
            goto EXIT_RTN;

EXPORT_ALL:
            for (int i = 0; i < list.Count; i++)
               PassThrough(ctx, list[i]);

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
            mapper = new MemoryBasedMapperWriters(null, null, 1);
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
