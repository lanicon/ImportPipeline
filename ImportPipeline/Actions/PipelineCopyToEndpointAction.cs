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
   public class PipelineCopyToEndpointAction : PipelineAction
   {
      protected String toField;
      protected String toFieldReal;
      protected String srcField;
      protected String srcFieldReal;
      protected String srcEndpointName;
      protected IDataEndpoint srcEndPoint;

      protected String sep;
      protected FieldFlags fieldFlags;
      protected bool clone;

      public PipelineCopyToEndpointAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         clone = node.ReadBool("@clone", false);
         toField = node.ReadStr("@field");
         srcField = node.ReadStr("@srcfield", toField);
         srcEndpointName = node.ReadStr("@srcendpoint");
         sep = node.ReadStr("@sep", null);
         fieldFlags = node.ReadEnum("@flags", sep == null ? FieldFlags.OverWrite : FieldFlags.Append);

         toFieldReal = toField == "*" ? null : toField;
         srcFieldReal = srcField == "*" ? null : srcField;
      }

      internal PipelineCopyToEndpointAction(PipelineCopyToEndpointAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.toField = optReplace(regex, name, template.toField);
         this.srcField = optReplace(regex, name, template.srcField);
         this.srcEndpointName = optReplace(regex, name, template.srcEndpointName);
         this.sep = template.sep;
         this.fieldFlags = template.fieldFlags;
         this.clone = template.clone;

         toFieldReal = toField == "*" ? null : toField;
         srcFieldReal = srcField == "*" ? null : srcField;
      }

      public override void Start(PipelineContext ctx)
      {
         base.Start(ctx);
         srcEndPoint = ctx.Pipeline.CreateOrGetDataEndpoint(ctx, srcEndpointName, postProcessors);
      }


      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         if (base.ConvertAndCallScriptNeeded)
         {
            value = ConvertAndCallScript(ctx, key, value);
            if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;
         }

         JToken fld = srcEndPoint.GetFieldAsToken(srcFieldReal);
         if (clone) fld = fld.DeepClone();
         endPoint.SetField(toFieldReal, fld, fieldFlags, sep);
         return value;
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", field={0}", toField);
         sb.AppendFormat(", srcfield={0}", srcField);
         sb.AppendFormat(", srcendpoint={0}", srcEndpointName);
         sb.AppendFormat(", clone={0}", clone);
      }

      
      public class Template : PipelineTemplate
      {
         public Template(Pipeline pipeline, XmlNode node)
            : base(pipeline, node)
         {
            template = new PipelineCopyToEndpointAction(pipeline, node);
         }

         public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
         {
            if (!regex.IsMatch(key)) return null;
            return new PipelineCopyToEndpointAction((PipelineCopyToEndpointAction)template, key, regex);
         }
      }
   }

}
