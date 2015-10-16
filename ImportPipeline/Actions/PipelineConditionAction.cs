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
using System.Diagnostics;

namespace Bitmanager.ImportPipeline
{
   public class PipelineConditionAction : PipelineAction
   {
      private readonly PipelineAction[] subActions;
      private readonly PipelineTemplate[] subTemplates;
      private readonly Condition cond;
      private readonly String skipUntil;
      public readonly int ActionsToSkip;
      private readonly bool genericSkipUntil;


      public PipelineConditionAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         XmlNodeList c = node.SelectNodes("action");
         if (c.Count > 2) throw new BMNodeException(node, "Max 2 sub-actions supported.");
         if (c.Count > 0)
         {
            subActions = createSubActions(pipeline, c);
         } 
         else
         {
            c = node.SelectNodes("template");
            if (c.Count > 2) throw new BMNodeException(node, "Max 2 sub-templates supported.");
            if (c.Count > 0)
            {
               subTemplates = createSubTemplates(pipeline, c);
            }
            else
            {
               skipUntil = node.ReadStr("@skipuntil");
               ActionsToSkip = node.ReadInt("@skip", 1);
               genericSkipUntil = skipUntil == "*";
            }
         }

         cond = Condition.Create(node);
      }

      private PipelineTemplate[] createSubTemplates(Pipeline pipeline, XmlNodeList c)
      {
         var ret = new PipelineTemplate[c.Count];
         for (int i = 0; i < c.Count; i++)
         {
            XmlElement elt = (XmlElement)c[i];
            elt.SetAttribute("expr", Name);
            ret[i] = PipelineTemplate.Create(pipeline, elt);
         }
         return ret;
      }

      private PipelineAction[] createSubActions(Pipeline pipeline, XmlNodeList c)
      {
         var ret = new PipelineAction[c.Count];
         for (int i = 0; i < c.Count; i++ )
         {
            XmlElement elt = (XmlElement)c[i];
            elt.SetAttribute("key", Name);
            ret[i] = PipelineAction.Create(pipeline, elt);
         }
         return ret;
      }

      internal PipelineConditionAction(PipelineContext ctx, PipelineConditionAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         skipUntil = optReplace(regex, name, template.skipUntil);
         genericSkipUntil = this.skipUntil == "*";
         
         String x = optReplace(regex, name, template.cond.Expression);
         cond = (x == template.cond.Expression) ? template.cond : Condition.Create(x);

         ActionsToSkip = template.ActionsToSkip;
         if (subTemplates != null)
         {
            subActions = new PipelineAction[subTemplates.Length];
            for (int i = 0; i < subActions.Length; i++)
            {
               subActions[i] = subTemplates[i].OptCreateAction(ctx, name);
            }
         }
         else
            subActions = template.subActions;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) goto EXIT_RTN;

         //if (endPoint.GetFieldAsStr("doc_cat") == "delete") Debugger.Break();

         bool matched = cond.NeedRecord ? cond.HasCondition ((JObject)endPoint.GetField(null)) : cond.HasCondition (value.ToJToken());
         if (subActions != null)
         {
            if (matched)
               return subActions[0].HandleValue(ctx, key, value);
            else
               if (subActions.Length > 1)
                  return subActions[1].HandleValue(ctx, key, value);
            return null;
         }

         if (matched)
         {
            if (skipUntil == null)
               ctx.ClearAllAndSetFlags(_ActionFlags.ConditionMatched);
            else
            {
               ctx.Skipped++;
               ctx.ClearAllAndSetFlags(_ActionFlags.SkipRest, genericSkipUntil ? Name : skipUntil);
            }
         }

         EXIT_RTN:
         return value;
      }

      public override void Start(PipelineContext ctx)
      {
         base.Start(ctx);
         if (subActions != null)
            foreach (var a in subActions)
               a.Start(ctx);
      }


      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         if (cond != null) sb.AppendFormat(", cond={0}", cond.Expression);
         if (skipUntil == null)
            sb.AppendFormat(", skip={0}", ActionsToSkip);
         else
            sb.AppendFormat(", skipuntil={0}", skipUntil);
      }

   }

   public class PipelineConditionTemplate : PipelineTemplate
   {
      public PipelineConditionTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineConditionAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineConditionAction(ctx, (PipelineConditionAction)template, key, regex);
      }
   }


}
