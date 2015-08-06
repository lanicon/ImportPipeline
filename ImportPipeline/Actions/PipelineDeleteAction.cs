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
   public class PipelineDeleteAction : PipelineAction
   {
      private enum _Condition {NonEmpty, Always, Test, Substring, Regex};
      private readonly String skipUntil;
      private readonly String testVal;
      private readonly String fromField;
      private readonly Regex expr;
      private readonly bool genericSkipUntil;
      private readonly _Condition cond;

      public PipelineDeleteAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         skipUntil = node.ReadStr("@skipuntil");
         testVal = node.ReadStr("@test", null);
         cond = node.ReadEnum("@cond", testVal == null ? _Condition.NonEmpty: _Condition.Test);
         genericSkipUntil = skipUntil == "*";
         fromField = node.ReadStr("@fromfield", null);
         switch (cond)
         {
            case _Condition.Always:
            case _Condition.NonEmpty: break;
            default:
               if (testVal == null) node.ReadStr("@test");
               break;
         }
         if (cond == _Condition.Regex) expr = new Regex(testVal, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
      }

      internal PipelineDeleteAction(PipelineDeleteAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.skipUntil = optReplace(regex, name, template.skipUntil);
         this.testVal = optReplace(regex, name, template.testVal);
         this.cond = template.cond;
         this.genericSkipUntil = this.skipUntil == "*";
         this.fromField = optReplace(regex, name, template.fromField);
         if (cond == _Condition.Regex) expr = new Regex(testVal, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) goto EXIT_RTN;

         if (fromField != null) value = endPoint.GetFieldAsStr(fromField);
         String v = value == null ? null : value.ToString();
         ctx.ImportLog.Log("del cond={0}, v={1} ff={2}", cond, v, fromField);
         switch (cond)
         {
            case _Condition.Always: goto SKIP;
            case _Condition.NonEmpty:
               if (String.IsNullOrEmpty(v)) goto EXIT_RTN;
               goto SKIP;
            case _Condition.Test:
               if (v == testVal) goto SKIP;
               goto EXIT_RTN;
            case _Condition.Regex:
               if (expr.IsMatch(v)) goto SKIP;
               goto EXIT_RTN;
            case _Condition.Substring:
               if (v!=null && v.IndexOf (testVal, StringComparison.OrdinalIgnoreCase) >= 0) goto SKIP;
               goto EXIT_RTN;
            default:
               cond.ThrowUnexpected();
               break;
         }

         SKIP:
         ctx.Skipped++;
         ctx.ClearAllAndSetFlags(_ActionFlags.SkipRest, genericSkipUntil ? Name : skipUntil);

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
