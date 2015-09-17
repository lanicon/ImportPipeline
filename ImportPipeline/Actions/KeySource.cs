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

using Bitmanager.Core;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public abstract class KeySource
   {
      public readonly String Input;
      public abstract String    GetKey(PipelineContext ctx, Object value);
      public abstract DateTime? GetKeyDate (PipelineContext ctx, Object value);

      protected KeySource (String input)
      {
         Input = input;
      }

      public override string ToString()
      {
         return Input;
      }
      protected static DateTime? toDateTime (Object value)
      {
         if (value == null) return null;
         JToken tk = value as JToken;
         if (tk != null) return (DateTime)tk;
         return (DateTime)value;
      }

      public static KeySource Parse(String ks)
      {
         if (String.IsNullOrEmpty(ks)) return null;

         if ("value".Equals(ks, StringComparison.OrdinalIgnoreCase)) return new KeySource_Value(ks);

         String[] arr = ks.Split(':');
         if (arr.Length != 2) goto INVALID;

         String rest;
         if (ks.StartsWith("var:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(4).Trim();
            if (String.IsNullOrEmpty(rest)) goto INVALID;
            return new KeySource_Var(ks, rest);
         }
         if (ks.StartsWith("field:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(6).Trim();
            if (String.IsNullOrEmpty(rest)) goto INVALID;
            return new KeySource_FieldExpr(ks, rest);
         }

         MemberTypes filter;
         if (ks.StartsWith("value:f:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(8).Trim();
            filter = MemberTypes.Field;
            goto CREATE_VALUE_EXPR;
         }
         if (ks.StartsWith("value:p:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(8).Trim();
            filter = MemberTypes.Property;
            goto CREATE_VALUE_EXPR;
         }
         if (ks.StartsWith("value:m:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(8).Trim();
            filter = MemberTypes.Method;
            goto CREATE_VALUE_EXPR;
         }
         if (ks.StartsWith("value:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(6).Trim();
            filter = MemberTypes.Method | MemberTypes.Property | MemberTypes.Field;
            goto CREATE_VALUE_EXPR;
         }

         goto INVALID;

         CREATE_VALUE_EXPR:
         if (String.IsNullOrEmpty(rest)) goto INVALID;
         return new KeySource_ValueExpr(ks, rest, filter);

         INVALID:
         throw new BMException ("Invalid keysource [{0}].", ks);
      }
   }

   public class KeySource_Value : KeySource
   {
      public KeySource_Value(String input)
         : base(input)
      {
      }

      public override String GetKey(PipelineContext ctx, Object value)
      {
         return value.ToString();
      }

      public override DateTime? GetKeyDate(PipelineContext ctx, Object value)
      {
         return toDateTime(value);
      }
   }
   public class KeySource_Var : KeySource
   {
      protected readonly String varkey;
      public KeySource_Var(String input, String varkey): base (input)
      {
         this.varkey = varkey;
      }

      public override String GetKey(PipelineContext ctx, Object value)
      {
         Object k = ctx.Pipeline.GetVariable(varkey);
         return k == null ? null : k.ToString();
      }

      public override DateTime? GetKeyDate(PipelineContext ctx, Object value)
      {
         return toDateTime(ctx.Pipeline.GetVariable(varkey));
      }
   }

   public class KeySource_ValueExpr : KeySource
   {
      static readonly Object[] noparms = new Object[0];
      protected PropertyInfo propInfo;
      protected FieldInfo fieldInfo;
      protected MethodInfo methodInfo;
      MemberTypes filter;
      protected readonly String expr;
      public KeySource_ValueExpr(String input, String expr, MemberTypes filter): base (input)
      {
         this.expr = expr;
         this.filter = filter;
      }

      protected Object getValue(Object input)
      {
         propInfo = null;
         fieldInfo = null;
         methodInfo = null;
         input.GetType().FindMembers(filter, BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance, filterMembers, null);
         if (propInfo != null) return propInfo.GetValue(input);
         if (methodInfo != null) return methodInfo.Invoke(input, noparms);
         if (fieldInfo != null) return fieldInfo.GetValue(input);
         throw new BMException("Object [{0}] doesn't have a [{1}]", input, base.Input);
      }
      public override String GetKey(PipelineContext ctx, Object value)
      {
         Object ret = getValue(value);
         return ret == null ? null : ret.ToString();
      }
      public override DateTime? GetKeyDate(PipelineContext ctx, Object value)
      {
         return toDateTime(getValue(value));
      }

      private bool filterMembers(MemberInfo m, object filterCriteria)
      {
         if (!expr.Equals(m.Name, StringComparison.OrdinalIgnoreCase)) return false;
         switch (m.MemberType)
         {
            case MemberTypes.Field:
               if (fieldInfo == null || m.Name == expr)
                  fieldInfo = (FieldInfo)m;
               break;
            case MemberTypes.Property:
               PropertyInfo pi = (PropertyInfo)m;
               if (pi.GetIndexParameters().Length != 0)break;
               if (propInfo == null || m.Name == expr)
                  propInfo = pi;
               break;
            case MemberTypes.Method:
               MethodInfo mi = (MethodInfo)m;
               if (mi.GetParameters().Length != 0) break;
               if (methodInfo == null || m.Name == expr)
                  methodInfo = mi;
              break;
         }
         return false;
      }
   }

   public class KeySource_FieldExpr : KeySource
   {
      protected readonly JPath expr;
      public KeySource_FieldExpr(String input, String expr): base (input)
      {
         this.expr = new JPath(expr);
      }

      public override String GetKey(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         return (String)expr.Evaluate ((JObject)value, JEvaluateFlags.NoExceptMissing);
      }
      public override DateTime? GetKeyDate(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         return (DateTime?)expr.Evaluate((JObject)value, JEvaluateFlags.NoExceptMissing);
      }
   }
}
