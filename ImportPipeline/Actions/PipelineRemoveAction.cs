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
   public class PipelineRemoveAction : PipelineAction
   {
      protected String fields;
      protected readonly String[] fieldArr;

      public PipelineRemoveAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         fields = node.ReadStr("@field");
         fieldArr = fields.SplitStandard();
      }

      internal PipelineRemoveAction(PipelineRemoveAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.fields = optReplace(regex, name, template.fields);
         fieldArr = fields.SplitStandard();
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         if (base.ConvertAndCallScriptNeeded)
         {
            value = ConvertAndCallScript(ctx, key, value);
            if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;
         }

         JObject obj = (JObject)Endpoint.GetField(null);
         foreach (var k in fieldArr)
            obj.Remove (k);
         return value;
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", field={0}", fields);
      }

      public class Template : PipelineTemplate
      {
         public Template(Pipeline pipeline, XmlNode node)
            : base(pipeline, node)
         {
            template = new PipelineRemoveAction(pipeline, node);
         }

         public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
         {
            if (!regex.IsMatch(key)) return null;
            return new PipelineRemoveAction((PipelineRemoveAction)template, key, regex);
         }
      }
   }

}
