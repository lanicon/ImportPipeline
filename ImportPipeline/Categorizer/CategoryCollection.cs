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
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;

namespace Bitmanager.ImportPipeline
{
   public class CategoryCollection : NamedItem
   {
      private enum CategoryMode { All, One };
      public readonly List<Category> Categories;
      private CategoryMode mode;

      public CategoryCollection(XmlNode node)
         : base(node)
      {
         XmlNodeList list = node.SelectNodes("category");
         Categories = new List<Category>(list.Count);
         foreach (XmlNode sub in list)
            Categories.Add(Category.Create (sub));
         mode = node.ReadEnum("@mode", CategoryMode.All);
      }

      public void HandleRecord(PipelineContext ctx)
      {
         IDataEndpoint ep = ctx.Action.Endpoint;
         JObject rec = (JObject)ep.GetField(null); 
         for (int i=0; i<Categories.Count; i++)
         {
            if (!Categories[i].HandleRecord(ctx, ep, rec)) continue;
            if (mode == CategoryMode.One) break;
         }
      }
   }
}
