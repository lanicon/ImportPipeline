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

      /// <summary>
      /// Calls the next processor (if any) and returns the total #added records by the last processor
      /// </summary>
      int CallNextPostProcessor(PipelineContext ctx);

      /// <summary>
      /// Passes the record to the next processor or endpoint
      /// </summary>
      void PassThrough(PipelineContext ctx, JObject value);

      /// <summary>
      /// Wraps a copy of the processor around the endpoint and returns the copy
      /// </summary>
      IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor);

      /// <summary>
      /// Returns the end Endpoint of the post-processor chain
      /// </summary>
      IDataEndpoint GetLastEndPoint();
   }

   public abstract class PostProcessorBase : JsonEndpointBase, IPostProcessor
   {
      public readonly String name;
      public String Name { get { return name; } }
      protected readonly IDataEndpoint nextEndpoint;
      protected readonly IPostProcessor nextProcessor;
      protected PostProcessorReport report;
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

      public virtual int CallNextPostProcessor(PipelineContext ctx)
      {
         var ret = cnt_added;
         if (report==null)
         {
            ReportStart(ctx);
            ReportEnd(ctx);
         }
         ctx.PostProcessor = this;
         if (nextProcessor != null) ret = nextProcessor.CallNextPostProcessor(ctx);
         return ret; 
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

      #endregion

      protected PostProcessorReport ReportStart(PipelineContext ctx)
      {
         report = new PostProcessorReport(this);
         if (ctx.DatasourceReport != null)
            ctx.DatasourceReport.AddPostProcessorReport(report);
         return report;
      }
      protected PostProcessorReport ReportEnd(PipelineContext ctx)
      {
         if (report != null)
         {
            report.Received = cnt_received;
            report.Passed = cnt_added;
            report.Skipped = cnt_skipped;
            report.MarkEnded(ctx);
         }
         return report;
      }

      public override abstract void Add(PipelineContext ctx);
      public abstract IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor);

   }
}
