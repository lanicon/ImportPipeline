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
using Bitmanager.Elastic;
using Bitmanager.ImportPipeline.Conditions;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Forward action to a different action
   /// </summary>
   public class PipelineForwardAction : PipelineAction
   {
      private readonly String forwardTo;
      private readonly bool clone;

      public PipelineForwardAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         forwardTo = node.ReadStr("@forward");
         clone = node.ReadBool("@clone", false);
      }

      internal PipelineForwardAction(PipelineForwardAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         forwardTo = optReplace(regex, name, template.forwardTo);
         clone = template.clone;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if (clone) value = JsonUtils.CloneToJson(value);
         return ((ctx.ActionFlags & _ActionFlags.Skip) != 0) ? null : ctx.Pipeline.HandleValue(ctx, forwardTo, value);
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", forward={0}", forwardTo);
      }

   }

   public class PipelineForwardTemplate : PipelineTemplate
   {
      public PipelineForwardTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineForwardAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineForwardAction((PipelineForwardAction)template, key, regex);
      }
   }


}
