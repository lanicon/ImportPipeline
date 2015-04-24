using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Text.RegularExpressions;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public class PipelineClearAction : PipelineAction
   {
      public PipelineClearAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      internal PipelineClearAction(PipelineClearAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) { ctx.Skipped++; goto EXIT_RTN; }

         endPoint.Clear();
         pipeline.ClearVariables();

      EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

   public class PipelineClearTemplate : PipelineTemplate
   {
      public PipelineClearTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineClearAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineClearAction((PipelineClearAction)template, key, regex);
      }
   }


}
