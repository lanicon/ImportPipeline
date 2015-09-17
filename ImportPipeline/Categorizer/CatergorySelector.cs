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
using Bitmanager.Core;
using Bitmanager.IO;
using Newtonsoft.Json.Linq;
using Bitmanager.ImportPipeline;
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline
{
   public interface ICategorySelector
   {
      bool IsSelected(JObject obj);
   }

   public abstract class CategorySelector : ICategorySelector
   {
      public readonly String Field;

      public CategorySelector(XmlNode node)
      {
         Field = node.ReadStr("@field");
      }

      private static bool hasChildSelectors (XmlNode node)
      {
         foreach (XmlNode c in node.ChildNodes)
         {
            if (c.NodeType != XmlNodeType.Element) continue;
            switch (c.Name)
            {
               case "or":
               case "and":
               case "not":
               case "select": return true;
            }
         }
         return false;
      }

      public static ICategorySelector CreateSelector(XmlNode node)
      {
         String name = node.Name;
         switch (name)
         {
            default: return null;
            case "or": return new CatergoryOrSelector(node);
            case "and": return new CatergoryAndSelector(node);
            case "select": return CreateValueSelector(node);
            case "not": return new CatergoryNotSelectorWrapper (CreateValueSelector(node));
         }
      }

      public static void CreateChildSelectors(List<ICategorySelector> coll, XmlNode node)
      {
         var children = node.ChildNodes;
         foreach (XmlNode c in node.ChildNodes)
         {
            if (c.NodeType != XmlNodeType.Element) continue;
            var sel = CreateSelector(c);
            if (sel == null) continue;
            coll.Add(sel);
         }
      }
      public static ICategorySelector CreateSelectors(XmlNode node)
      {
         if (!hasChildSelectors(node)) return CreateValueSelector(node);

         var list = new List<ICategorySelector>();
         CreateChildSelectors(list, node);
         if (list.Count == 1) return list[0];
         return new CatergoryOrSelector(list);
      }

      public static ICategorySelector CreateValueSelector(XmlNode node)
      {
         var elt = (XmlElement)node;
         if (elt.HasAttribute("dblrange")) return new CatergorySelectorDblRange(node);
         if (elt.HasAttribute("intrange")) return new CatergorySelectorIntRange(node);
         if (elt.HasAttribute("intvalue")) return new CatergorySelectorInt(node);
         if (elt.HasAttribute("range")) return new CatergorySelectorStringRange(node);
         return new CatergorySelectorString(node);
      }

      public virtual bool IsSelected(JObject obj)
      {
         return obj == null ? false : IsSelectedToken(obj.SelectToken(Field, false));
      }
      
      public virtual bool IsSelectedArr(JArray arr)
      {
         for (int i = 0; i < arr.Count; i++)
         {
            if (IsSelectedToken(arr[i])) return true;
         }
         return false;
      }

      public abstract bool IsSelectedToken(JToken val);
   }

}
