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
   public class PipelineSkipAction : PipelineAction
   {
      private enum _Condition {NonEmpty, Always, Test};
      private String skipUntil;
      private String testVal;
      private _Condition cond;

      public PipelineSkipAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         skipUntil = node.ReadStr("@skipuntil");
         testVal = node.ReadStr("@test", null);
         cond = node.ReadEnum("@cond", testVal == null ? _Condition.NonEmpty: _Condition.Test);
      }

      internal PipelineSkipAction(PipelineSkipAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.skipUntil = optReplace(regex, name, template.skipUntil);
         this.testVal = optReplace(regex, name, template.testVal);
         this.cond = template.cond;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) goto EXIT_RTN;

         switch (cond)
         {
            case _Condition.Always: goto SKIP;
            case _Condition.NonEmpty:
               if (value==null) goto EXIT_RTN;
               if (String.IsNullOrEmpty(value.ToString())) goto EXIT_RTN;
               goto SKIP;
            case _Condition.Test:
               String v = value==null ? null : value.ToString();
               if (v == testVal) goto SKIP;
               goto EXIT_RTN;
            default:
               cond.ThrowUnexpected();
               break;
         }

         SKIP:
         ctx.Skipped++;
         ctx.ClearAllAndSetFlags(_ActionFlags.SkipRest, skipUntil);

         EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

   public class PipelineSkipTemplate : PipelineTemplate
   {
      public PipelineSkipTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineSkipAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineSkipAction((PipelineSkipAction)template, key, regex);
      }
   }


}
