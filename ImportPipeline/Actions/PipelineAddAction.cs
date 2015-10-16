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
   public enum KeyCheckMode
   {
      key = 1, date = 2
   }
   public class PipelineAddAction : PipelineAction
   {
      private readonly Condition cond;
      public CategoryCollection[] Categories;
      public PipelineAddAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
          Categories = loadCategories(pipeline, node);
          cond = Condition.OptCreate(node);
      }

      internal PipelineAddAction(PipelineAddAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         Categories = template.Categories;
         if (template.cond != null)
         {
            String x = optReplace(regex, name, template.cond.Expression);
            cond = (x == template.cond.Expression) ? template.cond : Condition.Create(x);
         }
      }

      static CategoryCollection[] loadCategories(Pipeline pipeline, XmlNode node)
      {
         String[] cats = node.ReadStr("@categories", null).SplitStandard();
         if (cats == null || cats.Length == 0) return null;

         CategoryCollection[] list = new CategoryCollection[cats.Length];
         var engine = pipeline.ImportEngine;
         for (int i = 0; i < cats.Length; i++)
         {
            list[i] = engine.Categories.GetByName(cats[i]);
         }
         return list;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) { ctx.Skipped++; goto EXIT_RTN; }

         if (Categories != null)
            foreach (var cat in Categories) cat.HandleRecord(ctx);

         if (cond != null && !(cond.NeedRecord ? cond.HasCondition((JObject)endPoint.GetField(null)) : cond.HasCondition(value.ToJToken())))
         {
            ctx.Skipped++;
            goto CLEAR;
         }

         ctx.IncrementAndLogAdd();
         endPoint.Add(ctx);

      CLEAR:
         endPoint.Clear();
         pipeline.ClearVariables();

      EXIT_RTN:
         return value;
      }
   }

   public class PipelineAddTemplate : PipelineTemplate
   {
      public PipelineAddTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineAddAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineAddAction((PipelineAddAction)template, key, regex);
      }
   }


}
