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
using Bitmanager.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class ScriptExpressionHolder: IDisposable
   {
      int count;
      MemoryStream mem;
      StreamWriter wtr;
      private string className;
      public ScriptExpressionHolder (List<String> customUsings=null)
      {
         className = "_ScriptExpressions";
         mem = new MemoryStream();
         wtr = mem.CreateTextWriter();
         StringDict usings = new StringDict();
         usings.Add("System", null);
         usings.Add("System.IO", null);
         usings.Add("System.Linq", null);
         usings.Add("System.Text", null);
         usings.Add("System.Collections.Generic", null);
         usings.Add("Bitmanager.Core", null);
         usings.Add("Bitmanager.IO", null);
         usings.Add("Bitmanager.Json", null);

         if (customUsings != null)
            foreach (var u in customUsings)
            {
               String uu = u;
               if (uu.StartsWith("using ")) uu = u.Substring(6);
               if (uu.EndsWith(";")) uu = uu.Substring(0, uu.Length - 1);
               uu = uu.Trim();
               usings[uu] = null;
            }

         foreach (var kvp in usings)
         {
            wtr.Write("using ");
            wtr.Write(kvp.Key);
            wtr.WriteLine(";");
         }

         wtr.WriteLine();

         wtr.WriteLine("namespace Bitmanager.ImportPipeline");
         wtr.WriteLine("{");
         wtr.WriteLine("   public class _ScriptExpressions");
         wtr.WriteLine("   {");
      }

      public int Count { get { return count; } }
 
      public void SaveAndClose(String fn)
      {
         wtr.WriteLine("   }");
         wtr.WriteLine("}");
         wtr.Flush();
         using (FileStream fs = IOUtils.CreateOutputStream(fn))
            fs.Write(mem.GetBuffer(), 0, (int)mem.Length);
      }

      public void Close()
      {
         wtr.Dispose();
         mem.Dispose();
         wtr = null;
         mem = null;
      }


      public void Dispose()
      {
         Close();
      }

      private void writeMethodEntry(String name)
      {
         wtr.Write("      public Object ");
         wtr.Write(name);
         wtr.Write(" (PipelineContext ctx, Object value)\r\n      {\r\n");
      }
      private void writeMethodExit(bool appendSemiColon)
      {
         if (appendSemiColon) wtr.Write(';');
         wtr.WriteLine("\r\n      }\r\n");
      }
      public void AddExpression(String name, String expr)
      {
         ++count;
         writeMethodEntry(name);
         expr = expr.TrimWhiteSpace();
         wtr.Write("         ");
         if (expr.IndexOf("return ") < 0) wtr.Write("return ");
         wtr.Write(expr);
         writeMethodExit(!expr.EndsWith(";"));
      }

      public void AddCondition (String name, String expr)
      {
         if (expr.IndexOf("return ") >= 0) {
           AddExpression (name, expr);
            return;
         }

         ++count;
         writeMethodEntry(name);
         expr = expr.TrimWhiteSpace();
         if (expr.EndsWith(";")) 
            expr = expr.Substring(0, expr.Length - 1).TrimWhiteSpace();
         wtr.Write("         if (!(");
         wtr.Write(expr);
         wtr.Write("))  ctx.ActionFlags |= _ActionFlags.SkipAll;\r\n");
         wtr.Write("         return value;");
         writeMethodExit(false);
      }

      public string ClassName { get { return className; } }
      public string FullClassName { get { return "Bitmanager.ImportPipeline." + className; } }
   }
}
