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
   public class PipelineSplitAction : PipelineAction
   {
      private String prefix;
      private String preparedPrefix;
      private int splitUntil;
      private bool relative;
      public PipelineSplitAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         relative = node.ReadBool("@relative", true);
         splitUntil = node.ReadInt("@splituntil", 1);
         prefix = node.ReadStr ("@prefix", null);
         preparedPrefix = (prefix==null ? Name : prefix);
      }

      internal PipelineSplitAction(PipelineSplitAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.relative = template.relative;
         this.prefix = optReplace(regex, name, template.prefix);
         this.splitUntil = template.splitUntil;

         preparedPrefix = (prefix == null ? Name : prefix);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         Pipeline.SplitInnerTokens(ctx, ctx.Pipeline, (JToken)value, prefix, splitUntil);
         return value;
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", prefix={0}, splituntil={1}", prefix, splitUntil);
      }
   }

   public class PipelineSplitTemplate : PipelineTemplate
   {
      public PipelineSplitTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineSplitAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineSplitAction((PipelineSplitAction)template, key, regex);
      }
   }
}
