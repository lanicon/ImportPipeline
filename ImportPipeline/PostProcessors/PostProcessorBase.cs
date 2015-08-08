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
         nextEndpoint.Stop(ctx);
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
