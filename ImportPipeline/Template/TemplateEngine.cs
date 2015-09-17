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
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline.Template
{
   public class TemplateEngine
   {
      private MemoryStream mem;
      private TextWriter memWtr;
      public String FileName { get; set; }
      public String OutputFileName { get { return FileName == null ? null : FileName + ".generated.txt"; } }

      public int DebugLevel { get; set; }
      public bool AutoWriteGenerated { get; set; }
      private Logger logger;
      private IVariables fileVariables;
      public IVariables Variables { get; set; }
      public IVariables FileVariables { get {return fileVariables; } }

      public TemplateEngine(ITemplateSettings settings)
      {
         if (settings != null)
         {
            Variables = settings.InitialVariables;
            DebugLevel = settings.DebugLevel;
            AutoWriteGenerated = settings.AutoWriteGenerated;
         }
         if (Variables==null) Variables = new Variables();
         logger = Logs.DebugLog.Clone("TemplateEngine");
      }

      public void LoadFromFile (String fn)
      {
         if (DebugLevel > 10)
         {
            logger.Log();
            logger.Log("LoadFromFile ({0})", fn);
         }
         mem = new MemoryStream();
         memWtr = IOUtils.CreateTextWriter(mem);
         var ctx = new ParseContextFile(null, Variables, fn);
         FileName = ctx.FileName;
         evaluateContent(ctx);
         fileVariables = ctx.Vars;
         memWtr.Flush();
         if (AutoWriteGenerated) WriteGeneratedOutput();
      }

      public String WriteGeneratedOutput ()
      {
         String outputFn = OutputFileName;
         using (var fs = File.Create(outputFn))
         {
            fs.Write(mem.GetBuffer(), 0, (int)mem.Length);
         }
         return outputFn;
      }

      public String ResultAsString()
      {
         memWtr.Flush();
         return Encoding.UTF8.GetString(mem.GetBuffer(), 0, (int)mem.Length);
      }
      public Stream ResultAsStream()
      {
         memWtr.Flush();
         mem.Position = 0;
         return mem;
      }
      public TextReader ResultAsReader()
      {
         return ResultAsStream().CreateTextReader();
      }

      private void write (String content, int startOffset, int endOffset)
      {
         for (int i=startOffset; i<endOffset; i++) memWtr.Write(content[i]);
      }

      private static String getPrefix (int lvl)
      {
         switch (lvl)
         {
            case 0: return null;
            case 1: return "-- ";
            case 2: return "-- -- ";
            case 3: return "-- -- -- ";
         }
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < lvl; i++) sb.Append("-- ");
         return sb.ToString();
      }
      private void evaluateContent (ParseContext ctx)
      {
         if (DebugLevel>10)  logger.Log("{0} Evaluate {1}", getPrefix(2*ctx.Level), ctx.GetOrigin());
         String content = ctx.Content;
         int idx0=-1;
         int idx1=0;
         while (idx1 < content.Length)
         {
            idx0 = content.IndexOf ("$$", idx1);
            if (idx0 < 0) idx0 = content.Length;
            write (content, idx1, idx0);  //Write everything before the first $$

            idx0 += 2;
            if (idx0 >= content.Length) return;
            idx1 = content.IndexOf ("$$", idx0);
            if (idx1 < 0)
            {
               idx1 = content.Length;
               break;
            }

            if (!processDirective(ctx, content, idx0, idx1))  
            {
               idx1 += 2;
               continue;
            }

            //Remove EOL in case of a readl directive
            idx1 += 2;
            int scanUntil = idx1+2;
            if (scanUntil > content.Length) scanUntil = content.Length;
            for (; idx1<scanUntil; idx1++)
            {
               switch (content[idx1])
               {
                  case '\r': continue; 
                  case '\n': continue; 
               }
               break;
            }
         }
         write(content, idx1, content.Length);  //Write last part
      }



      private void addMissed (ParseContext ctx, String key)
      {

      }
      private bool processDirective(ParseContext ctx, String content, int idx0, int idx1)
      {
         if (idx0 == idx1) return false;
         String key = content.Substring(idx0, idx1 - idx0);
         if (DebugLevel > 10) logger.Log("{0} process directive: {1}", getPrefix(1 + 2 * ctx.Level), key);
         
         if (key[0] != '#')
         {
            Object repl = ctx.Vars.Get(key);
            if (repl==null) 
               addMissed (ctx, key);
            else
               evaluateContent(new ParseContextVar(ctx, ctx.Vars, key, repl.ToString()));
            return false;
         }

         if (key == "#restore")
            ctx.RestoreVars();
         else if (key.StartsWith("#debug "))
            processDebug (ctx, key, sub(key, 7));
         else if (key.StartsWith("#define "))
            processDefine(ctx, key, sub(key, 8));
         else if (key.StartsWith("#undefine "))
            processDefine(ctx, key, sub(key, 10));
         else if (key.StartsWith("#undef "))
            processUnDefine(ctx, key, sub(key, 7));
         else if (key.StartsWith("#include "))
            processInclude(ctx, key, sub(key, 9));
         else if (key.StartsWith("//")) //comment
            goto EXIT_RTN;
         else if (key.StartsWith("_")) //comment
            goto EXIT_RTN;
         else
            ctx.Throw("Unknown template directive '{0}'.", key);
         EXIT_RTN:
         return true;
      }
      private static String sub (String s, int offset)
      {
         return s.Substring(offset).Trim();
      }

      private void processInclude(ParseContext ctx, String rawDirective, String directive)
      {
         String fn = directive.Trim();
         evaluateContent(new ParseContextFile(ctx, ctx.Vars, fn));
      }

      private void processDebug(ParseContext ctx, String rawDirective, String directive)
      {
         AutoWriteGenerated = true;
         Match m = defineExpr1.Match(directive);
         if (m.Success)
         {
            String k = m.Groups[1].Value;
            if (!String.Equals("lvl", k, StringComparison.OrdinalIgnoreCase) && !String.Equals("level", k, StringComparison.OrdinalIgnoreCase))
               throw new BMException("Invalid #debug directive {0}. Only lvl or level is allowed.", rawDirective);
            DebugLevel = Invariant.ToInt32(m.Groups[1].Value, 0);
            return;
         }
      }

      private void processUnDefine(ParseContext ctx, String rawDirective, String directive)
      {
         if (ctx.Vars == ctx.OrgVars) return;
         String key = directive;
         ctx.Vars.Set(key, ctx.OrgVars.Get(key));
         //if (Debug) logger.Log("{0} {1} restored to {2}", getPrefix(1 + 2 * ctx.Level), key, ctx.OrgVars.Get(key));
      }

      static Regex defineExpr1 = new Regex(@"^\s*([^:=\s]*)\s*[:=]\s*(.*)\s*$");
      static Regex defineExpr2 = new Regex(@"^\s*([^:=\s]*)\s*$");
      private void processDefine(ParseContext ctx, String rawDirective, String directive)
      {
         Match m = defineExpr1.Match(directive);
         if (m.Success)
         {
            ctx.OwnVars().Set(m.Groups[1].Value, m.Groups[2].Value);
            return;
         }
         m = defineExpr2.Match(directive);
         if (m.Success)
         {
            ctx.OwnVars().Set(m.Groups[1].Value, null);
            return;
         }

         ctx.Throw("Syntax error in define directive: #define {0}", directive); 
      }
   }

   abstract class ParseContext
   {
      public readonly ParseContext Parent;
      public readonly IVariables OrgVars;
      public IVariables Vars;
      public readonly String Content;
      public readonly int Level;

      public ParseContext(ParseContext parent, IVariables vars, String content)
      {
         if (parent != null) Level = parent.Level+1;
         Parent = parent;
         Vars = OrgVars = vars;
         Content = content;
         if (Level > 255) Throw("Recursion too deep: {0}.", Level);
      }

      public void RestoreVars()
      {
         Vars = OrgVars;
      }

      public IVariables OwnVars()
      {
         if (Vars != OrgVars) return Vars;
         Vars = OrgVars.Clone();
         return Vars;
      }

      public abstract String GetOrigin();
      public void Throw(String msg)
      {
         msg = msg + "\r\n" + GetOrigin() + ".";
         Logger logger = Logs.ErrorLog;
         logger.Log(_LogType.ltError, msg);
         StringBuilder sb = new StringBuilder();
         sb.Append("Trace:");
         String prevOrigin = null;
         int prevOriginCnt = 0;
         for (ParseContext p = this; p != null; p = p.Parent)
         {
            String o = p.GetOrigin();
            if (o == prevOrigin)
            {
               ++prevOriginCnt;
               continue;
            }

            appendLine(sb, prevOriginCnt);
            sb.Append("-- ");
            sb.Append(o);
            prevOrigin = o;
            prevOriginCnt = 0;
         }
         appendLine(sb, prevOriginCnt);
         logger.Log(sb);
         throw new BMException(msg);
      }

      private static void appendLine (StringBuilder sb, int cnt)
      {
         if (cnt > 0) 
            sb.AppendFormat (" ({0} times)", cnt+1);
         sb.AppendLine();
      }
      public void Throw(String msg, params Object[] args)
      {
         String txt = String.Format(msg, args);
         Throw(txt);
      }
   }
   class ParseContextFile : ParseContext
   {
      public readonly String FileName;
      public readonly String BaseDir;

      public ParseContextFile(ParseContext parent, IVariables vars, String fn)
         : base(parent, vars, File.ReadAllText(fn = getFullFileName(parent, fn)))
      {
         FileName = fn;
         BaseDir = Path.GetDirectoryName(fn);
      }

      protected static String getFullFileName (ParseContext parent, String fn)
      {
         String baseDir = getBaseDir (parent);
         if (baseDir == null) return Path.GetFullPath(fn);

         return IOUtils.FindFileToRoot(Path.GetFullPath(Path.Combine(baseDir, fn)), FindToTootFlags.ReturnOriginal);
      }

      protected static String getBaseDir (ParseContext parent)
      {
         for (; parent != null; parent = parent.Parent)
         {
            var pcf = parent as ParseContextFile;
            if (pcf != null) return pcf.BaseDir;
         }
         return null;
      }

      public override String GetOrigin()
      {
         return "File: [" + FileName + "]";
      }

   }

   class ParseContextVar : ParseContext
   {
      public readonly String VarName;

      public ParseContextVar(ParseContext parent, IVariables vars, String key, String content)
         : base(parent, vars, content)
      {
         VarName = key;
      }

      public override String GetOrigin()
      {
         return "Variable: [" + VarName + "]";
      }

   }
}
