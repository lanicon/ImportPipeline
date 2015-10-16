/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
   public class PipelineFieldAction2 : PipelineAction
   {
      protected String toField;
      protected String toFieldReal;
      protected String toFieldFromVar;
      protected String toVar;
      protected FieldFlags fieldFlags;
      protected String sep;

      public PipelineFieldAction2(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         toVar = node.ReadStr("@tovar", null);
         toField = node.ReadStr("@field", null);
         toFieldFromVar = node.ReadStr("@fieldfromvar", null);
         sep = node.ReadStr("@sep", null);
         fieldFlags = node.ReadEnum("@flags", sep == null ? FieldFlags.OverWrite : FieldFlags.Append);

         if (toField == null && toVar == null && base.scriptName == null && toFieldFromVar == null)
            throw new BMNodeException(node, "At least one of 'field', 'toFieldFromVar', 'tovar', 'script'-attributes is mandatory.");

         toFieldReal = toField == "*" ? null : toField;

         if (node.ReadStr ("@fromvar", null) != null || node.ReadStr("@fromfield", null) != null || node.ReadStr("@fromvalue", null) != null)
            throw new BMNodeException (node, "fromvar|fromfield|fromvalue not supported. Use source=xxxx or type='orgfield' for the deprecated action.");
      }

      internal PipelineFieldAction2(PipelineFieldAction2 template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.toField = optReplace(regex, name, template.toField);
         this.toVar = optReplace(regex, name, template.toVar);
         this.toFieldFromVar = optReplace(regex, name, template.toFieldFromVar);
         this.sep = template.sep;
         this.fieldFlags = template.fieldFlags;
         toFieldReal = toField == "*" ? null : toField;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;

         if (toField != null)
            endPoint.SetField(toFieldReal, value, fieldFlags, sep);
         else
            if (toFieldFromVar != null)
            {
               String fn = ctx.Pipeline.GetVariableStr(toFieldFromVar);
               if (fn != null) endPoint.SetField(fn, value, fieldFlags, sep);
            }
         if (toVar != null) ctx.Pipeline.SetVariable(toVar, value);
         return base.PostProcess(ctx, value);
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         if (toField != null)
            sb.AppendFormat(", field={0}", toField);
         else if (toFieldFromVar != null)
            sb.AppendFormat(", fieldfromvar={0}", toFieldFromVar);
         if (toVar != null)
            sb.AppendFormat(", tovar={0}", toVar);
      }
   }



   public class PipelineFieldTemplate2 : PipelineTemplate
   {
      public PipelineFieldTemplate2(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineFieldAction2(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineFieldAction2((PipelineFieldAction2)template, key, regex);
      }
   }
}
