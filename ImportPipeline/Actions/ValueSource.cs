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
   /// <summary>
   /// ValueSource that extracts a value from 
   /// - a value
   /// - a property/field/method of that value
   /// - a value in the cached variables
   /// - a value from the Json record
   /// </summary>
   public abstract class ValueSource
   {
      public static readonly ValueSource Default = new ValueSource_Value("value");
      public readonly String Input;
      public abstract Object GetValue(PipelineContext ctx, Object value);

      protected ValueSource (String input)
      {
         Input = input;
      }

      public override string ToString()
      {
         return Input;
      }

      public static ValueSource Parse(String ks)
      {
         if (String.IsNullOrEmpty(ks)) return null;

         if ("value".Equals(ks, StringComparison.OrdinalIgnoreCase)) return Default;

         String[] arr = ks.Split(':');
         if (arr.Length < 2 || arr.Length > 3) goto INVALID;

         String rest;
         if (ks.StartsWith("var:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(4).Trim();
            if (String.IsNullOrEmpty(rest)) goto INVALID;
            return new ValueSource_Var(ks, rest);
         }
         if (ks.StartsWith("field:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(6).Trim();
            if (String.IsNullOrEmpty(rest)) goto INVALID;
            return new ValueSource_JsonExpr(ks, rest);
         }

         if (ks.StartsWith("record:", StringComparison.OrdinalIgnoreCase))
         {
            rest = ks.Substring(6).Trim();
            if (String.IsNullOrEmpty(rest)) return new ValueSource_Record(ks);
            return new ValueSource_RecordJsonExpr(ks, rest);
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
         return new ValueSource_ValueExpr(ks, rest, filter);

         INVALID:
         throw new BMException ("Invalid valuesource [{0}].\nShould be in format 'value:(p|m|f|):xxx|field:xxx|var:xxx|record:xxx", ks);
      }
   }

   /// <summary>
   /// ValueSource that extracts the value from a value that was supplied to the pipeline
   /// </summary>
   public class ValueSource_Value : ValueSource
   {
      public ValueSource_Value(String input)
         : base(input)
      {
      }

      public override Object GetValue(PipelineContext ctx, Object value)
      {
         return value;
      }

   }

   /// <summary>
   /// ValueSource that extracts the value from cached variables in the pipeline.
   /// </summary>
   public class ValueSource_Var : ValueSource
   {
      protected readonly String varkey;
      public ValueSource_Var(String input, String varkey)
         : base(input)
      {
         this.varkey = varkey;
      }

      public override Object GetValue(PipelineContext ctx, Object value)
      {
         return ctx.Pipeline.GetVariable(varkey);
      }
   }

   /// <summary>
   /// ValueSource that extracts the value from a sub[field/prop/method] of a value that was supplied to the pipeline
   /// </summary>
   public class ValueSource_ValueExpr : ValueSource
   {
      protected Func<Object, Object> getter;
      protected readonly MemberTypes filter;
      protected readonly String expr;

      public ValueSource_ValueExpr(String input, String expr, MemberTypes filter): base (input)
      {
         this.expr = expr;
         this.filter = filter;
      }

      public override Object GetValue(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         return getLambda(value)(value);
      }

      private Func<Object, Object> getLambda(Object o)
      {
         return getter != null ? getter : createLambda(o);
      }
      private Func<Object, Object> createLambda(Object o)
      {
         return getter = o.GetType().GetBestMember(expr, filter, ReflectionExtensions.DefaultBinding | BindingFlags.NonPublic).CreateGetterLambda<Object, Object>();
      }
   }

   /// <summary>
   /// ValueSource that extracts the value from a Json object
   /// </summary>
   public class ValueSource_JsonExpr : ValueSource
   {
      protected readonly JPath expr;
      public ValueSource_JsonExpr(String input, String expr)
         : base(input)
      {
         this.expr = new JPath(expr);
      }

      public override Object GetValue(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         return expr.Evaluate((JObject)value, JEvaluateFlags.NoExceptMissing);
      }
   }

   /// <summary>
   /// ValueSource that extracts the value from the associated record object
   /// </summary>
   public class ValueSource_RecordJsonExpr : ValueSource
   {
      protected readonly JPath expr;
      public ValueSource_RecordJsonExpr(String input, String expr)
         : base(input)
      {
         this.expr = new JPath(expr);
      }

      public override Object GetValue(PipelineContext ctx, Object value)
      {
         value = ctx.Action.Endpoint.GetField(null);
         return value == null ? null : expr.Evaluate((JObject)value, JEvaluateFlags.NoExceptMissing);
      }
   }

   /// <summary>
   /// ValueSource that extracts the value from the associated full record object
   /// </summary>
   public class ValueSource_Record : ValueSource
   {
      public ValueSource_Record(String input)
         : base(input)
      {
      }

      public override Object GetValue(PipelineContext ctx, Object value)
      {
         return ctx.Action.Endpoint.GetField(null);
      }
   }
}
