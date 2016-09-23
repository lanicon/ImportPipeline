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
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class ScriptExpressionHolder: IDisposable
   {
      enum Needed { None, Endpoint = 1, Record = 2, Action = 4, PostProcessor = 8 };
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
         usings.Add("System.Xml", null);
         usings.Add("System.Collections.Generic", null);
         usings.Add("Bitmanager.Core", null);
         usings.Add("Bitmanager.IO", null);
         usings.Add("Bitmanager.Json", null);
         usings.Add("Bitmanager.Elastic", null);
         usings.Add("Bitmanager.ImportPipeline", null);
         usings.Add("Bitmanager.ImportPipeline.StreamProviders", null);
         usings.Add("Newtonsoft.Json.Linq", null);

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

      public static String GenerateScriptName(String what, String contextName, XmlNode node)
      {
         StringBuilder sb = new StringBuilder();
         sb.Append(what);
         sb.Append('_');
         sb.Append(contextName);
         sb.Append('_');
         sb.Append(node.Name);
         sb.Append('_');
         XmlNodeList list = node.ParentNode.SelectNodes(node.Name);
         for (int i = 0; i < list.Count; i++)
            if (list[i] == node) { sb.Append(i); break; }
         return sb.ToString();
      }

      private void writeMethodEntry(String name, String code)
      {
         Needed neededVars = checkNeeded(code);
         wtr.Write("      public Object ");
         wtr.Write(name);
         wtr.Write(" (PipelineContext ctx, Object value)\r\n      {\r\n");
         if ((neededVars &  (Needed.Action | Needed.Endpoint | Needed.Record)) != 0)
            wtr.Write("         var action = ctx.Action;\r\n");
         if ((neededVars & (Needed.Endpoint | Needed.Record)) != 0)
            wtr.Write("         var endpoint = action.Endpoint;\r\n");
         if ((neededVars & (Needed.Record)) != 0)
            wtr.Write("         var record = (JObject)endpoint.GetFieldAsToken (null);\r\n");
      }
      private void writeMethodExit(bool appendSemiColon)
      {
         if (appendSemiColon) wtr.Write(';');
         wtr.WriteLine("\r\n      }\r\n");
      }

      /// <summary>
      /// Adds an expression for a standard pipeline action
      /// Signature: func(PipelineContext ctx, Object value)
      /// </summary>
      public void AddExpression(String name, String expr)
      {
         ++count;
         writeMethodEntry(name, expr);
         expr = expr.TrimWhiteSpace();
         wtr.Write("         ");
         if (expr.IndexOf("return ") < 0) wtr.Write("return ");
         wtr.Write(expr);
         writeMethodExit(!expr.EndsWith(";"));
      }

      /// <summary>
      /// Adds an expression for an Undup-action
      /// Signature: func(PipelineContext ctx, List&lt;JObject&gt; records, int offset, int len)
      /// </summary>
      public void AddUndupExpression(String name, String code)
      {
         ++count;
         code = code.TrimWhiteSpace();
         Needed neededVars = checkNeeded(code);
         wtr.Write("      public void ");
         wtr.Write(name);
         wtr.Write(" (PipelineContext ctx, List<JObject> records, int offset, int len)\r\n      {\r\n");
         if ((neededVars & (Needed.PostProcessor)) != 0)
            wtr.Write("         var processor = ctx.PostProcessor;\r\n");
         
         wtr.Write("         ");
         wtr.Write(code);
         writeMethodExit(!code.EndsWith(";"));

         //if ((neededVars & (Needed.PostProcessor | Needed.Endpoint | Needed.Record)) != 0)
         //   wtr.Write("         var processor = ctx.PostProcessor;\r\n");
         //if ((neededVars & (Needed.Endpoint | Needed.Record)) != 0)
         //   wtr.Write("         var endpoint = action.Endpoint;\r\n");
         //if ((neededVars & (Needed.Record)) != 0)
         //   wtr.Write("         var record = (JObject)endpoint.GetFieldAsToken (null);\r\n");
      }

      /// <summary>
      /// Adds a condition for a standard pipeline action
      /// Signature: func(PipelineContext ctx, Object value)
      /// The expression itself should evaluate to a bool! This bool will be translated into the appropriate flags in the ctx
      /// </summary>
      public void AddCondition(String name, String expr)
      {
         if (expr.IndexOf("return ") >= 0) {
            AddExpression (name, expr);
            return;
         }

         ++count;
         writeMethodEntry(name, expr);
         expr = expr.TrimWhiteSpace();
         if (expr.EndsWith(";")) 
            expr = expr.Substring(0, expr.Length - 1).TrimWhiteSpace();
         wtr.Write("         if (!(");
         wtr.Write(expr);
         wtr.Write("))  ctx.ActionFlags |= _ActionFlags.SkipAll;\r\n");
         wtr.Write("         return value;");
         writeMethodExit(false);
      }

      private static bool equalSubstring (String sub, String str, int offs)
      {
         for (int i=0; i<sub.Length; i++)
         {
            if (sub[i] != str[i + offs]) return false;
         }
         return true;
      }
      private static Needed checkNeeded (String code, int offs, int len)
      {
         Logs.ErrorLog.Log("CHECKNEEDED ({0}) len={1}", code.Substring(offs, len), len);
         switch (len)
         {
            case 6:
               if (equalSubstring("record", code, offs)) return Needed.Record;
               if (equalSubstring("action", code, offs)) return Needed.Record;
               break;
            case 8:
               if (equalSubstring("endpoint", code, offs)) return Needed.Endpoint;
               break;
            case 13:
               if (equalSubstring("postprocessor", code, offs)) return Needed.PostProcessor;
               break;
         }
         return Needed.None;
      }
      private static Needed checkNeeded (String code)
      {
         var ret = Needed.None;

         int start = -1;
         for (int i=0; i<code.Length; i++)
         {
            switch (code[i])
            {
               case '\n':
               case '\r':
               case '\t':
               case ';':
               case ' ':
               case '.':
               case '[':
               case '=':
                  if (start < 0) continue;
                  int len = i-start;
                  ret |= checkNeeded (code, start, len);
                  start = i+1;
                  continue;
               default:
                  if (start < 0) start = i;
                  continue;
            }
         }
         if (start < code.Length)
            ret |= checkNeeded (code, start, code.Length-start);
         return ret; 
      }

      public string ClassName { get { return className; } }
      public string FullClassName { get { return "Bitmanager.ImportPipeline." + className; } }
   }
}
