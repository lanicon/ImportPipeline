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

      public int DebugLevel {get; set;}
      private Logger logger;
      public IVariables Variables {get; set;}

      public TemplateEngine()
      {
         Variables = new Variables();
         logger = Logs.DebugLog.Clone("TemplateEngine");
      }
      public TemplateEngine(int debugLevel)
         : this()
      {
         DebugLevel = debugLevel;
      }
      public TemplateEngine(IVariables vars, int debugLevel=0)
      {
         logger = Logs.DebugLog.Clone("TemplateEngine");
         Variables = vars != null ? vars : new Variables();
         DebugLevel = debugLevel;
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
         memWtr.Flush();

         if (DebugLevel > 0)
         {
            using (var fs=File.Create (ctx.FileName + ".generated.xml"))
            {
               fs.Write(mem.GetBuffer(), 0, (int)mem.Length);
            }
         }
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
         else if (key.StartsWith("#define "))
            processDefine(ctx, key, sub(key,8));
         else if (key.StartsWith("#undefine "))
            processDefine(ctx, key, sub(key, 10));
         else if (key.StartsWith("#undef "))
            processUnDefine(ctx, key, sub(key, 7));
         else if (key.StartsWith("#include "))
            processInclude(ctx, key, sub(key,9));
         else
            ctx.Throw("Unknown template directive '{0}'.", key);
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

      private void processUnDefine(ParseContext ctx, String rawDirective, String directive)
      {
         if (ctx.Vars == ctx.OrgVars) return;
         String key = directive;
         ctx.Vars.Set (key, ctx.OrgVars.Get (key));
         //if (Debug) logger.Log("{0} {1} restored to {2}", getPrefix(1 + 2 * ctx.Level), key, ctx.OrgVars.Get(key));
      }

      static Regex defineExpr1 = new Regex(@"^\s*([^\s]*)\s*[:=]\s*(.*)\s*$");
      static Regex defineExpr2 = new Regex(@"^\s*([^\s]*)\s*$");
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
