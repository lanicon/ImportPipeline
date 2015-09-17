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
   public class CatergorySelectorString : CategorySelector
   {
      private enum SelectMode { None, Value, Contains, Expr, StartsWith, EndsWith };
      public readonly Regex Expr;
      public readonly String Value;
      SelectMode mode;

      private void checkDupMode(XmlNode node)
      {
         if (mode != SelectMode.None)
            throw new BMNodeException(node, "Ambigious selector. [expr, value, contains, startswith, endwith] are mutually exclusive.");
      }
      public CatergorySelectorString(XmlNode node)
         : base(node)
      {
         XmlAttributeCollection attrs = ((XmlElement)node).Attributes;
         mode = SelectMode.None;
         foreach (XmlAttribute att in attrs)
         {
            switch (att.Name)
            {
               case "expr":
                  checkDupMode(node);
                  mode = SelectMode.Expr;
                  Value = att.Value;
                  Expr = new Regex(Value, RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
                  continue;
               case "value":
                  checkDupMode(node);
                  mode = SelectMode.Value;
                  Value = att.Value;
                  continue;
               case "contains":
                  checkDupMode(node);
                  mode = SelectMode.Contains;
                  Value = att.Value;
                  continue;
               case "startswith":
                  checkDupMode(node);
                  mode = SelectMode.StartsWith;
                  Value = att.Value;
                  continue;
               case "endswith":
                  checkDupMode(node);
                  mode = SelectMode.EndsWith;
                  Value = att.Value;
                  continue;
            }
         }
         if (mode == SelectMode.None)
            throw new BMNodeException(node, "One of [expr, value, contains, startswith, endwith] should be specified.");
      }


      public override bool IsSelectedToken(JToken val)
      {
         if (val == null) return false;
         switch (val.Type)
         {
            case JTokenType.Array:
               return IsSelectedArr((JArray)val);
            default:
               return IsSelectedValue(val.ToString());
         }
      }

      private bool IsSelectedValue(String v)
      {
         switch (mode)
         {
            case SelectMode.Value:
               return String.Equals(Value, v, StringComparison.InvariantCultureIgnoreCase);
            case SelectMode.Contains:
               if (v == null) return false;
               return v.IndexOf(Value, StringComparison.InvariantCultureIgnoreCase) >= 0;
            case SelectMode.StartsWith:
               if (v == null) return false;
               return v.StartsWith(Value, StringComparison.InvariantCultureIgnoreCase);
            case SelectMode.EndsWith:
               if (v == null) return false;
               return v.EndsWith(Value, StringComparison.InvariantCultureIgnoreCase);
            case SelectMode.Expr:
               if (v == null) return false;
               return Expr.IsMatch(v);
         }
         return false;
      }
   }

   public class CatergorySelectorStringRange : CategorySelector
   {
      public readonly StringRange Range;
      public CatergorySelectorStringRange(XmlNode node)
         : base(node)
      {
         Range = new StringRange(node.ReadStr("@range"));
      }

      public override bool IsSelectedToken(JToken val)
      {
         if (val == null) return false;
         switch (val.Type)
         {
            case JTokenType.Array:
               return IsSelectedArr((JArray)val);
            case JTokenType.Object:
               return false;
            default:
               return Range.IsInRange(val.ToString());
         }
      }
   }
}
