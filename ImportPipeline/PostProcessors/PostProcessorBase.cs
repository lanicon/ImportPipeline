using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline.PostProcessors
{
   public interface IPostProcessor
   {
      void CallNextPostProcessor(PipelineContext ctx);
   }

   public abstract class PostProcessorBase : JsonEndpointBase<Endpoint>, IPostProcessor
   {
      private bool autoCallNextPostProcessor;
      private IDataEndpoint nextEndpoint;

      public PostProcessorBase(): base(null) {}
      public abstract void CallNextPostProcessor(PipelineContext ctx);

      public override void Start(PipelineContext ctx)
      {
         nextEndpoint.Start(ctx);
      }

      public override void Stop(PipelineContext ctx)
      {
         if (autoCallNextPostProcessor)
            CallNextPostProcessor(ctx);
      }

      public override abstract void Add(PipelineContext ctx);

   }
}
