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
   public interface IPostProcessor
   {
      String Name { get; }
      void CallNextPostProcessor(PipelineContext ctx);
      IPostProcessor Clone(IDataEndpoint epOrnextProcessor);
   }

   public abstract class PostProcessorBase : JsonEndpointBase, IPostProcessor
   {
      public readonly String name;
      public String Name { get { return name; } }
      protected readonly IDataEndpoint nextEndpoint;
      protected readonly IPostProcessor nextProcessor;
      private bool autoCallNextPostProcessor;

      public PostProcessorBase(ImportEngine engine, XmlNode node) {
         name = node.ReadStr("@name");
      }

      public PostProcessorBase(PostProcessorBase other, IDataEndpoint epOrnextProcessor)
      {
         this.name = other.name;
         this.nextEndpoint = epOrnextProcessor;
         this.nextProcessor = epOrnextProcessor as IPostProcessor;
         this.autoCallNextPostProcessor = other.autoCallNextPostProcessor;
      }

      public virtual void CallNextPostProcessor(PipelineContext ctx)
      {
         if (nextProcessor != null) nextProcessor.CallNextPostProcessor(ctx);
      }

      public override void Start(PipelineContext ctx)
      {
         nextEndpoint.Start(ctx);
      }

      public override void Stop(PipelineContext ctx)
      {
         nextEndpoint.Stop(ctx);
      }

      public override abstract void Add(PipelineContext ctx);
      public abstract IPostProcessor Clone(IDataEndpoint epOrnextProcessor);

   }
}
