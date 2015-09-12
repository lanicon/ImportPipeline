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
using Bitmanager.ImportPipeline.StreamProviders;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public class PipelineCheckExistAction : PipelineAction
   {
      private readonly KeySource keySource;
      private readonly KeySource dateSource;

      public PipelineCheckExistAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         keySource = KeySource.Parse(node.ReadStr("@keysource"));
         dateSource = KeySource.Parse(node.ReadStr("@datesource", null));
      }

      internal PipelineCheckExistAction(PipelineCheckExistAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         String x = optReplace(regex, name, template.keySource.Input);
         keySource = (x == template.keySource.Input) ? template.keySource : KeySource.Parse(x);
         if (template.dateSource != null)
         {
            x = optReplace(regex, name, template.dateSource.Input);
            dateSource = (x == template.dateSource.Input) ? template.dateSource : KeySource.Parse(x);
         }
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         ExistState ret = ExistState.NotExist;
         String k = keySource.GetKey(ctx, value);
         if (k != null)
         {
            DateTime? dt = dateSource == null ? null : dateSource.GetKeyDate(ctx, value);
            ret = endPoint.Exists(ctx, k, dt);
         }
         PostProcess(ctx, value);
         return ret;

      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", keysource={0}", keySource);
         sb.AppendFormat(", datesource={0}", dateSource);
      }
   }



   public class PipelineCheckExistTemplate : PipelineTemplate
   {
      public PipelineCheckExistTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineCheckExistAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineCheckExistAction((PipelineCheckExistAction)template, key, regex);
      }
   }
}
