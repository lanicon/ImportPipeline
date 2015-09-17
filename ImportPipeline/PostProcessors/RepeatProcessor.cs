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
   public class RepeatProcessor : PostProcessorBase
   {
      private int repeatCount;
      private int cntIn, cntOut;

      public RepeatProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         repeatCount = node.ReadInt("@repeat");
      }

      public RepeatProcessor(PipelineContext ctx, RepeatProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         repeatCount = other.repeatCount;
      }


      public override IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor)
      {
         return new RepeatProcessor(ctx, this, epOrnextProcessor);
      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("{0} [type={1}, clone=#{2}, repeat={3}]",
            Name, GetType().Name, InstanceNo, repeatCount);
         return sb.ToString();
      }

      private void dumpStats(PipelineContext ctx)
      {
         Logger logger = ctx.ImportLog;
         logger.Log("PostProcessor {0} ended.", this);
         logger.Log("-- In={0}, out={1}.", cntIn, cntOut);
      }
      public override void Stop(PipelineContext ctx)
      {
         dumpStats(ctx);
         base.Stop(ctx);
      }

      public override void Add(PipelineContext ctx)
      {
         if (accumulator.Count > 0)
         {
            cntIn++;
            for (int i = 0; i < repeatCount; i++)
            {
               cntOut++;
               nextEndpoint.SetField(null, accumulator.DeepClone());
               nextEndpoint.Add(ctx);
            }
            Clear();
         }
      }

   }
}
