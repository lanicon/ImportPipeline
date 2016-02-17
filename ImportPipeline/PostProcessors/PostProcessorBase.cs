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
using Bitmanager.Elastic;
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
   public interface IPostProcessor
   {
      String Name { get; }
      void CallNextPostProcessor(PipelineContext ctx);
      void PassThrough (PipelineContext ctx, JObject value);
      IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor);
      IDataEndpoint GetLastEndPoint();
   }

   public abstract class PostProcessorBase : JsonEndpointBase, IPostProcessor
   {
      public readonly String name;
      public String Name { get { return name; } }
      protected readonly IDataEndpoint nextEndpoint;
      protected readonly IPostProcessor nextProcessor;
      private int instanceNo; //Unique number per clone
      public int InstanceNo { get { return instanceNo; } }
      protected int cnt_added;
      protected int cnt_skipped;
      protected int cnt_received;

      public PostProcessorBase(ImportEngine engine, XmlNode node) {
         name = node.ReadStr("@name");
         instanceNo = -1;
      }

      public PostProcessorBase(PostProcessorBase other, IDataEndpoint epOrnextProcessor)
      {
         this.name = other.name;
         this.nextEndpoint = epOrnextProcessor;
         this.nextProcessor = epOrnextProcessor as IPostProcessor;
         instanceNo = ++other.instanceNo;
      }

      public virtual void CallNextPostProcessor(PipelineContext ctx)
      {
         ctx.PostProcessor = this;
         if (nextProcessor != null) nextProcessor.CallNextPostProcessor(ctx);
      }

      public virtual void PassThrough(PipelineContext ctx, JObject value)
      {
         ++cnt_added;
         nextEndpoint.SetField(null, value);
         nextEndpoint.Add(ctx);
      }

      public virtual IDataEndpoint GetLastEndPoint()
      {
         return nextProcessor == null ? nextEndpoint : nextProcessor.GetLastEndPoint();
      }

      #region Passing through important methods of the endpoint
      public override void Start(PipelineContext ctx)
      {
         nextEndpoint.Start(ctx);
      }

      public override void Stop(PipelineContext ctx)
      {
         dumpStats(ctx);
         nextEndpoint.Stop(ctx);
      }

      protected virtual String getStatsLine()
      {
         return String.Format("-- In={0}, out={1}, skipped={2}.", cnt_received, cnt_added, cnt_skipped);
      }
      protected virtual void dumpStats(PipelineContext ctx)
      {
         Logger logger = ctx.ImportLog;
         logger.Log("PostProcessor {0} ended.", this);
         logger.Log(getStatsLine());
      }

      public override ExistState Exists(PipelineContext ctx, string key, DateTime? timeStamp)
      {
         return nextEndpoint.Exists(ctx, key, timeStamp);
      }

      public override Object LoadRecord(PipelineContext ctx, String key)
      {
         return nextEndpoint.LoadRecord(ctx, key);
      }

      public override IAdminEndpoint GetAdminEndpoint(PipelineContext ctx)
      {
         IEndpointResolver epr = nextEndpoint as IEndpointResolver;
         return epr == null ? null : epr.GetAdminEndpoint(ctx);
      }
      #endregion


      public override abstract void Add(PipelineContext ctx);
      public abstract IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor);

   }
}
