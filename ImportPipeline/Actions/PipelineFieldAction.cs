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
   public class PipelineFieldAction : PipelineAction
   {
      protected enum FieldSource { Value, Field, Event, Variable };
      protected String toField;
      protected String toFieldFromVar;
      protected String toVar;
      protected String fromVar;
      protected String fromField;
      protected String fromValue;
      protected FieldFlags fieldFlags;
      protected String sep;
      protected FieldSource fieldSource;

      public PipelineFieldAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         fieldSource = FieldSource.Event;
         toVar = node.ReadStr("@tovar", null);
         fromVar = node.ReadStr("@fromvar", null);
         fromField = node.ReadStr("@fromfield", null);
         fromValue = node.ReadStr("@fromvalue", null);

         int cnt=0;
         if (fromVar != null)
         {
            cnt++;
            fieldSource = FieldSource.Variable;
         }
         if (fromField != null)
         {
            cnt++;
            fieldSource = FieldSource.Field;
         }
         if (fromValue != null)
         {
            cnt++;
            fieldSource = FieldSource.Value;
         }
         if (cnt > 1)
            throw new BMNodeException(node, "Cannot specify fromvar, fromfield or fromvalue together.");

         toField = node.ReadStr("@field", null);
         toFieldFromVar = node.ReadStr("@fieldfromvar", null);
         sep = node.ReadStr("@sep", null);
         fieldFlags = node.ReadEnum("@flags", sep == null ? FieldFlags.OverWrite : FieldFlags.Append);

         if (checkMode == 0 && toField == null && toVar == null && base.scriptName == null && toFieldFromVar == null)
            throw new BMNodeException(node, "At least one of 'field', 'toFieldFromVar', 'tovar', 'script', 'check'-attributes is mandatory.");
      }

      internal PipelineFieldAction(PipelineFieldAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.toField = optReplace(regex, name, template.toField);
         this.toVar = optReplace(regex, name, template.toVar);
         this.fromVar = optReplace(regex, name, template.fromVar);
         this.fromField = optReplace(regex, name, template.fromField);
         this.fromValue = optReplace(regex, name, template.fromValue);
         this.toFieldFromVar = optReplace(regex, name, template.toFieldFromVar);
         this.sep = template.sep;
         this.fieldFlags = template.fieldFlags;
         this.fieldSource = template.fieldSource;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         switch (fieldSource)
         {
            case FieldSource.Field: value = endPoint.GetField(fromField); break;
            case FieldSource.Value: value = fromValue; break;
            case FieldSource.Variable: value = ctx.Pipeline.GetVariable(fromVar); break;
         }
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;

         if (toField != null)
            endPoint.SetField(toField, value, fieldFlags, sep);
         else
            if (toFieldFromVar != null)
            {
               String fn = ctx.Pipeline.GetVariableStr(toFieldFromVar);
               if (fn != null) endPoint.SetField(fn, value, fieldFlags, sep);
            }
         if (toVar != null) ctx.Pipeline.SetVariable(toVar, value);
         return base.handleCheck(ctx, value);
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         if (toField != null)
            sb.AppendFormat(", field={0}", toField);
         else if (toFieldFromVar != null)
            sb.AppendFormat(", fieldfromvar={0}", toFieldFromVar);
         if (fromVar != null)
            sb.AppendFormat(", fromvar={0}", fromVar);
         if (fromVar != null)
            sb.AppendFormat(", fromfield={0}", fromField);
         if (toVar != null)
            sb.AppendFormat(", tovar={0}", toVar);
      }
   }



   public class PipelineFieldTemplate : PipelineTemplate
   {
      public PipelineFieldTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineFieldAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineFieldAction((PipelineFieldAction)template, key, regex);
      }
   }
}
