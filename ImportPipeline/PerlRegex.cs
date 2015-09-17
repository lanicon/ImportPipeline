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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline
{
   public class PerlRegex
   {
      Regex expr;
      String repl;
      bool isReplace;

      public PerlRegex(String expr, bool mustReplace=true )
      {
         if (String.IsNullOrEmpty(expr)) goto ERR_SYNTAX;

         switch (expr[0])
         {
            default: goto ERR_SYNTAX;
            case 'm':
               if (mustReplace) goto ERR_NO_REPL;
               break;
            case 's':
               isReplace = true;
               break;
         }
         String[] parts = expr.Split(expr[1]);
         if (parts.Length < 2) goto ERR_SYNTAX;

         if (mustReplace && parts.Length < 3) goto ERR_NO_REPL;

         this.expr = new Regex(parts[1]);
         this.repl = parts[2];
         return;

      ERR_SYNTAX: throw new BMException("Invalid PerlRegex expression [{0}]. Must be formed like '<m|s>/<expr>/<repl>/<switches>'.", expr);
      ERR_NO_REPL: throw new BMException("PerlRegex expression [{0}] is not a replace expression..", expr);
      }

      public bool IsMatch(String data)
      {
         return data == null ? false : expr.IsMatch(data);
      }
      public String Replace (String data)
      {
         if (data == null) return data;
         return expr.Replace(data, repl);
      }

      public static bool IsMatch(String expr, String data)
      {
         if (expr == null) return false;
         return new PerlRegex(expr, false).IsMatch(data);
      }
      public static String Replace (String expr, String data)
      {
         if (expr == null) return data;
         return new PerlRegex(expr).Replace(data);
      }
   }
}
