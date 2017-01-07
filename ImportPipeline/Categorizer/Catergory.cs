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

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Json;

namespace Bitmanager.ImportPipeline
{
   public abstract class Category
   {
      public Category[] SubCats;
      public readonly ICategorySelector Selector;
      public readonly String Field;
      public readonly bool FieldIsEvent;
      public Category(XmlNode node)
      {
         Selector = CategorySelector.CreateSelectors(node);
         Field = node.ReadStr ("@dstfield");
         FieldIsEvent = Field.Contains('/');

         XmlNodeList subNodes = node.SelectNodes(node.Name);
         if (subNodes.Count > 0)
         {
            SubCats = new Category[subNodes.Count];
            for (int i = 0; i < subNodes.Count; i++) SubCats[i] = Create(subNodes[i]);
         }
     
      }

      public abstract bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec);

      protected void HandleSubcats (PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (SubCats == null) return;
         for (int i = 0; i < SubCats.Length; i++) SubCats[i].HandleRecord(ctx, ep, rec);
      }

      public static Category Create (XmlNode node)
      {
         var elt = (XmlElement)node;
         if (elt.HasAttribute("intcat")) return new IntCategory(node);
         if (elt.HasAttribute("dblcat")) return new DblCategory(node);
         return new StringCategory(node);
      }

   }

   public class StringCategory : Category
   {
      public readonly String Value;

      public StringCategory(XmlNode node)
         : base(node)
      {
         Value = node.ReadStr("@cat");
      }

      public override bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (!Selector.IsSelected(rec)) return false;

         if (SubCats != null) HandleSubcats(ctx, ep, rec);
         if (Value != null)
         {
            if (FieldIsEvent)
               ctx.Pipeline.HandleValue(ctx, Field, Value);
            else
               ep.SetField(Field, Value, FieldFlags.Append, ";");
         }
         return true;
      }
   }

   public class IntCategory : Category
   {
      public readonly long Value;

      public IntCategory(XmlNode node)
         : base(node)
      {
         Value = node.ReadInt64("@intcat");
      }

      public override bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (!Selector.IsSelected(rec)) return false;

         if (SubCats != null) HandleSubcats(ctx, ep, rec);
         if (FieldIsEvent)
            ctx.Pipeline.HandleValue(ctx, Field, Value);
         else
            ep.SetField(Field, Value, FieldFlags.ToArray);
         return true;
      }
   }

   public class DblCategory : Category
   {
      public readonly double Value;

      public DblCategory(XmlNode node)
         : base(node)
      {
         Value = node.ReadFloat("@dblcat");
      }

      public override bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (!Selector.IsSelected(rec)) return false;

         if (SubCats != null) HandleSubcats(ctx, ep, rec);
         if (FieldIsEvent)
            ctx.Pipeline.HandleValue(ctx, Field, Value);
         else
            ep.SetField(Field, Value, FieldFlags.OverWrite);
         return true;
      }
   }
}
