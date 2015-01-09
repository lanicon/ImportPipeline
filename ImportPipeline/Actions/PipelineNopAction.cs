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

namespace Bitmanager.ImportPipeline
{
   public class PipelineNopAction : PipelineAction
   {
      public PipelineNopAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }
      public PipelineNopAction(String name) : base(name) { }

      internal PipelineNopAction(PipelineNopAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         return null;
      }
   }

   public class PipelineNopTemplate : PipelineTemplate
   {
      public PipelineNopTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineNopAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineNopAction((PipelineNopAction)template, key, regex);
      }
   }

}
