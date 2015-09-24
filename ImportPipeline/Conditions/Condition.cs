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
using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline.Conditions
{
   public abstract class Condition
   {
      public enum ConditionType { String = 1, Int = 2, Double = 3, SubString = 4, Regex = 5, IsNull = 6, IsNullOrEmpty = 7, CaseSensitive = 0x1000, Not = 0x2000, LT = 0x4000, GT = 0x8000, EQ = 0x10000, NE=Not, LTE=Not|GT, GTE=Not|LT };
      protected const ConditionType Types = ConditionType.String | ConditionType.Double | ConditionType.Int | ConditionType.SubString | ConditionType.Regex | ConditionType.IsNull | ConditionType.IsNullOrEmpty;
      protected const ConditionType Operators = ConditionType.LT | ConditionType.EQ | ConditionType.GT;
      protected ConditionType type;

      protected JPath Field;
      protected String rawValue;
      public readonly String Expression;
      public readonly bool NeedRecord;

      protected Condition (String expr, JPath fld, ConditionType type, String rawValue)
      {
         this.Expression = expr;
         this.Field = fld;
         this.type = type;
         if (rawValue != null)
            NeedRecord = true;
         this.rawValue = rawValue;
      }

      protected bool toBool(int cmpResult)
      {
         bool ret = true;
         switch (type & (ConditionType.EQ | ConditionType.GT | ConditionType.LT))
         {
            case ConditionType.EQ:
               ret = cmpResult == 0;
               break;
            case ConditionType.LT:
               ret = cmpResult < 0;
               break;
            case ConditionType.GT:
               ret = cmpResult > 0;
               break;
         }

         return (type & ConditionType.Not) == 0 ? ret : !ret;
      }
      protected bool toBool(bool cmpResult)
      {
         return (type & ConditionType.Not) == 0 ? cmpResult : !cmpResult;
      }
      protected void CheckOnlyEQ()
      {
         if ((type & Operators) != ConditionType.EQ)
            throw new BMException("{0} only allows EQ-operator.", this.GetType().Name);
      }

      public virtual bool HasCondition(JObject tk)
      {
         if (Field == null) throw new BMException("Condition: cannot compare object without a field-specifier.");
         return HasCondition(Field.Evaluate(tk, JEvaluateFlags.NoExceptMissing));
      }

      public abstract bool HasCondition(JToken tk);
      //public abstract bool HasCondition(Object v);

      private static String readCond (XmlNode node, bool needed)
      {
         String c = node.ReadStr("@condition", null);
         if (c != null) return c;

         return needed ? node.ReadStr("@cond") : node.ReadStr("@cond", null);
      }

      public static Condition Create(XmlNode node)
      {
         return Create(readCond(node, true));
      }
      public static Condition OptCreate(XmlNode node)
      {
         var c = readCond(node, false);
         return c == null ? null : Create(c, node);
      }
      public static Condition Create(String cond, XmlNode node = null)
      {
         String[] arr = cond.Split(',');//"field,eq|aaa,123" "eq,123"   "field, eq"
         String fld = null;
         String rawValue = null;
         ConditionType type;

         switch (arr.Length)
         {
            case 1:  //only conditiontype
               type = Invariant.ToEnum<ConditionType>(arr[0]);
               break;
            case 2:  //field and condition
               fld = String.IsNullOrEmpty(arr[0]) ? null : arr[0].Trim();
               type = Invariant.ToEnum<ConditionType>(arr[1]);
               break;
            case 3:  //field, condition and value
               fld = String.IsNullOrEmpty(arr[0]) ? null : arr[0].Trim();
               type = Invariant.ToEnum<ConditionType>(arr[1]);
               rawValue = arr[2];
               break;
            default:
               throw new BMNodeException(node, "Invalid condition [{0}]: must be formed like <field, cond, value>.", cond);
         }
         if ((type & Operators) == 0) type |= ConditionType.EQ;
         if ((type & Types) == 0) type |= ConditionType.String;

         JPath path = fld == null ? null : new JPath(fld);

         switch ((ConditionType)(type & (ConditionType.CaseSensitive-1)))
         {
            default:
            case ConditionType.String:
               if (String.IsNullOrEmpty (rawValue))
                  return new NullOrEmptyCondition(cond, path, type, rawValue);
               return new StringCondition(cond, path, type, rawValue);
   
            case ConditionType.IsNull:
               return new NullCondition(cond, path, type, rawValue);
            case ConditionType.IsNullOrEmpty:
               return new NullOrEmptyCondition(cond, path, type, rawValue);

            case ConditionType.SubString:
               return new SubStringCondition(cond, path, type, rawValue);
            case ConditionType.Regex:
               return new RegexCondition(cond, path, type, rawValue);
            case ConditionType.Double:
               return new DoubleCondition(cond, path, type, rawValue);
            case ConditionType.Int:
               return new LongCondition(cond, path, type, rawValue);

         }
      }
   }

   public class NullCondition : Condition
   {
      public NullCondition(String expr, JPath fld, ConditionType type, String rawValue)
         : base(expr, fld, type, rawValue)
      {
         CheckOnlyEQ();
      }

      public override bool HasCondition(JToken tk)
      {
         return toBool((tk == null || tk.Type == JTokenType.Null || tk.Type == JTokenType.Undefined));
      }
   }
   public class NullOrEmptyCondition : Condition
   {
      public NullOrEmptyCondition(String expr, JPath fld, ConditionType type, String rawValue)
         : base(expr, fld, type, rawValue)
      {
         CheckOnlyEQ();
      }

      public override bool HasCondition(JToken tk)
      {
         bool res;
         if (tk == null)
            res = true;
         else
         {
            switch (tk.Type)
            {
               default:
                  res = false;
                  break;
               case JTokenType.Null:
               case JTokenType.Undefined:
                  res = true;
                  break;
               case JTokenType.String:
                  res = String.IsNullOrEmpty((String)tk);
                  break;
            }
         }
         return toBool(res);
      }
   }

   public class StringCondition : Condition
   {
      protected StringComparison comparison;
      //RawValue cannot be null here....
      public StringCondition(String expr, JPath fld, ConditionType type, String rawValue)
         : base(expr, fld, type, rawValue)
      {
         if (String.IsNullOrEmpty(rawValue)) throw new BMException("RawValue for StringCondition should not be null or empty.");
         comparison = (type & ConditionType.CaseSensitive) == 0 ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
      }

      public override bool HasCondition(JToken tk)
      {
         int res;
         if (tk == null)
         {
            res = -1;
            goto EXIT_RTN;
         }
         String str;
         switch (tk.Type)
         {
            default:
               str = tk.ToString();
               break;
            case JTokenType.Null:
            case JTokenType.Undefined:
               res = -1;
               goto EXIT_RTN;
            case JTokenType.String:
               str = (String)tk;
               break;
         }

         if ((type & Operators) == ConditionType.EQ)
            return toBool(str.Equals(rawValue, comparison));
         else
            res = String.Compare (str, rawValue, comparison);
      EXIT_RTN:
         return toBool(res);
      }
   }

   public class SubStringCondition : StringCondition
   {
      public SubStringCondition(String expr, JPath fld, ConditionType type, String rawValue)
         : base(expr, fld, type, rawValue)
      {
      }

      public override bool HasCondition(JToken tk)
      {
         bool res;
         if (tk == null)
         {
            res = false;
            goto EXIT_RTN;
         }
         String str;
         switch (tk.Type)
         {
            default:
               str = tk.ToString();
               break;
            case JTokenType.Null:
            case JTokenType.Undefined:
               res = false;
               goto EXIT_RTN;
            case JTokenType.String:
               str = (String)tk;
               break;
         }

         res = str.IndexOf(rawValue, comparison) >= 0;
      EXIT_RTN:
         return toBool(res);
      }
   }

   public class RegexCondition : SubStringCondition
   {
      Regex expr;
      public RegexCondition(String text, JPath fld, ConditionType type, String rawValue)
         : base(text, fld, type, rawValue)
      {
         var options = RegexOptions.CultureInvariant | RegexOptions.Compiled;
         if ((type & ConditionType.CaseSensitive) == 0)
            options |= RegexOptions.IgnoreCase;
         expr = new Regex (rawValue, options);
      }

      public override bool HasCondition(JToken tk)
      {
         bool res;
         if (tk == null)
         {
            res = false;
            goto EXIT_RTN;
         }
         String str;
         switch (tk.Type)
         {
            default:
               str = tk.ToString();
               break;
            case JTokenType.Null:
            case JTokenType.Undefined:
               res = false;
               goto EXIT_RTN;
         }

         res = expr.IsMatch(str);
      EXIT_RTN:
         return toBool(res);
      }
   }


   public class DoubleCondition : Condition
   {
      double testValue;
      public DoubleCondition(String expr, JPath fld, ConditionType type, String rawValue)
         : base(expr, fld, type, rawValue)
      {
         testValue = Invariant.ToDouble(rawValue);
      }

      public override bool HasCondition(JToken tk)
      {
         int res;
         if (tk == null)
         {
            res = -1;
            goto EXIT_RTN;
         }

         switch (tk.Type)
         {
            default:
               res = Comparer<double>.Default.Compare((double)tk, testValue);
               break;
            case JTokenType.Null:
            case JTokenType.Undefined:
               res = -1;
               goto EXIT_RTN;
         }

      EXIT_RTN:
         return toBool(res);
      }
   }

   public class LongCondition : Condition
   {
      long testValue;
      public LongCondition(String expr, JPath fld, ConditionType type, String rawValue)
         : base(expr, fld, type, rawValue)
      {
         testValue = Invariant.ToInt64(rawValue);
      }

      public override bool HasCondition(JToken tk)
      {
         int res;
         if (tk == null)
         {
            res = -1;
            goto EXIT_RTN;
         }

         switch (tk.Type)
         {
            default:
               res = Comparer<long>.Default.Compare((long)tk, testValue);
               break;
            case JTokenType.Null:
            case JTokenType.Undefined:
               res = -1;
               goto EXIT_RTN;
         }

      EXIT_RTN:
         return toBool(res);
      }
   }
}
