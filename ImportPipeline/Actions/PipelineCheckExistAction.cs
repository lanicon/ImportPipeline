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
      protected Selector fieldExtracter;
      protected KeyCheckMode keyCheckMode;
      protected String idField;
      public PipelineCheckExistAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         idField = node.ReadStr ("idfield", null);
         checkMode = node.ReadEnum<KeyCheckMode>("@check", 0);
         if (idField == null && checkMode==0) throw new BMNodeException (node, "At least idfield or checkmode should be specified.");

         if (checkMode == KeyCheckMode.date) checkMode |= KeyCheckMode.key;
         initExtractor();
      }

      private void initExtractor ()
      {
         if (idField != null) fieldExtracter = new Selector(null, idField, true, JEvaluateFlags.NoExceptWrongType | JEvaluateFlags.NoExceptMissing);
      }

      internal PipelineCheckExistAction(PipelineCheckExistAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.idField = optReplace(regex, name, template.idField);
         this.checkMode = template.checkMode;
         if (this.idField == template.idField)
            this.fieldExtracter = template.fieldExtracter;
         else
            initExtractor();
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         ExistState ret = ExistState.NotExist;
         var p = value as IStreamProvider;
         String k;
         if (p != null)
         {
            k = fieldExtracter==null ? p.FullName : (String)fieldExtracter.ConvertScalar (ctx, p);
            ret = endPoint.Exists(ctx, k, p.LastModified);
            goto EXIT_RTN;
         }


         k = (String)ctx.Pipeline.GetVariable("key");
         if (k != null)
         {
            Object date = null;
            if ((checkMode & KeyCheckMode.date) != 0)
               date = ctx.Pipeline.GetVariable("date");
            ret = endPoint.Exists(ctx, (String)k, (DateTime?)date);
            goto EXIT_RTN;
         }
         return null;

      EXIT_RTN: ;
         PostProcess(ctx, value);
         return ret;

      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         if (idField != null)
            sb.AppendFormat(", idfield={0}", idField);
         sb.AppendFormat(", checkmode={0}", checkMode);
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
