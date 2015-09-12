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
using Bitmanager.ImportPipeline.Conditions;

namespace Bitmanager.ImportPipeline
{
   public class PipelineDeleteAction : PipelineAction
   {
      private readonly KeySource keySource;
      private readonly String skipUntil;
      private readonly Condition cond;
      private readonly bool genericSkipUntil;

      public PipelineDeleteAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         skipUntil = node.ReadStr("@skipuntil");
         cond = Condition.OptCreate(node);
         genericSkipUntil = skipUntil == "*";

         keySource = KeySource.Parse (node.ReadStr("@keysource", null));
      }

      internal PipelineDeleteAction(PipelineDeleteAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         skipUntil = optReplace(regex, name, template.skipUntil);
         genericSkipUntil = this.skipUntil == "*";
         if (template.cond != null)
         {
            String x = optReplace(regex, name, template.cond.Expression);
            cond = (x == template.cond.Expression) ? template.cond : Condition.Create(x);
         }
         if (template.keySource != null)
         {
            String x = optReplace(regex, name, template.keySource.Input);
            keySource = (x == template.keySource.Input) ? template.keySource : KeySource.Parse(x);
         }
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) goto EXIT_RTN;

         if (cond == null || (cond.NeedRecord ? cond.HasCondition((JObject)endPoint.GetField(null)) : cond.HasCondition(value.ToJToken())))
         {
            ctx.Skipped++;
            ctx.ClearAllAndSetFlags(_ActionFlags.SkipRest, genericSkipUntil ? Name : skipUntil);
            if (keySource != null)
            {
               String k = keySource.GetKey(ctx, value);
               if (k != null) endPoint.Delete(ctx, k);
            }
         }
         EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

   public class PipelineDeleteTemplate : PipelineTemplate
   {
      public PipelineDeleteTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineDeleteAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineDeleteAction((PipelineDeleteAction)template, key, regex);
      }
   }


}
