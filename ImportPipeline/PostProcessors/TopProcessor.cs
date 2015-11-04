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
   public class TopProcessor : PostProcessorBase
   {
      private readonly JComparer sorter;
      FixedPriorityQueue<JObject> prique;

      int topCount;
      bool reverse;


      public TopProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("sorter"), "key", true);
         sorter = JComparer.Create(list);
         topCount = node.ReadInt ("@count");
         reverse = node.ReadBool ("@reverse", false);
      }


      //public delegate int Comparison<in T>(T x, T y);

      public TopProcessor(PipelineContext ctx, TopProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         sorter = other.sorter;
         reverse = other.reverse;
         topCount = other.topCount;
         prique = new FixedPriorityQueue<JObject>(topCount, ComparisonWrappers.Create (sorter, !reverse));
      }


      public override IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor)
      {
         return new TopProcessor(ctx, this, epOrnextProcessor);
      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("{0} [type={1}, clone=#{2}, sorter={3}, count={4}, reverse={5}]",
            Name, GetType().Name, InstanceNo, sorter, topCount, reverse);
         return sb.ToString();
      }

      private void dumpStats(PipelineContext ctx)
      {
         Logger logger = ctx.ImportLog;
         logger.Log("PostProcessor {0} ended.", this);
         //logger.Log("-- In={0}, out={1}, passed through={2}, sorted={3}, after undup={4}.", numAfterSort, numAfterUndup, 0, numAfterSort, numAfterUndup);
      }
      public override void Stop(PipelineContext ctx)
      {
         dumpStats(ctx);
         base.Stop(ctx);
      }


      public override void CallNextPostProcessor(PipelineContext ctx)
      {
         ctx.PostProcessor = this;
         int N = prique.Count;
         prique.SortDestructive(sorter); 
         for (int i = 0; i < N; i++)
         {
            nextEndpoint.SetField(null, prique[i]);
            nextEndpoint.Add(ctx);
         }
         base.CallNextPostProcessor(ctx);
      }

      public override void Add(PipelineContext ctx)
      {
         if (accumulator.Count > 0)
         {
            prique.Add(accumulator);
            Clear();
         }
      }

   }
}
