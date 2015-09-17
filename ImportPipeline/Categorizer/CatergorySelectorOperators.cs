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

namespace Bitmanager.ImportPipeline
{
   public class CatergoryAndSelector : ICategorySelector
   {
      private List<ICategorySelector> items;

      public CatergoryAndSelector(System.Xml.XmlNode node)
      {
         items = new List<ICategorySelector>();
         CategorySelector.CreateChildSelectors(items, node);
      }

      public bool IsSelected(JObject obj)
      {
         for (int i = 0; i < items.Count; i++)
         {
            if (!items[i].IsSelected(obj)) return false;
         }
         return items.Count > 0;
      }

   }

   public class CatergoryOrSelector : ICategorySelector
   {
      private List<ICategorySelector> items;

      public CatergoryOrSelector(XmlNode node)
      {
         items = new List<ICategorySelector>();
         CategorySelector.CreateChildSelectors(items, node);
      }

      public CatergoryOrSelector(List<ICategorySelector> list)
      {
         items = list;
      }

      public bool IsSelected(JObject obj)
      {
         for (int i = 0; i < items.Count; i++)
         {
            if (items[i].IsSelected(obj)) return true;
         }
         return false;
      }

   }

   public class CatergoryNotSelectorWrapper : ICategorySelector
   {
      private readonly ICategorySelector wrapped;

      public CatergoryNotSelectorWrapper(ICategorySelector other)
      {
         wrapped = other;
      }

      public bool IsSelected(JObject obj)
      {
         return !wrapped.IsSelected(obj);
      }

   }
}
