using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.IO;
using System.Text.RegularExpressions;
using System.Reflection;
using System.CodeDom.Compiler;
using System.Runtime.InteropServices;
using Microsoft.CSharp;

namespace Bitmanager.ImportPipeline
{
   [Flags]
   public enum ScriptHostFlags
   {
      None = 0,
      AddLoadedAsReference = 1<<0,
      WarningAsError = 1<<1,
      ExceptOnError = 1<<2,
      Default = AddLoadedAsReference | ExceptOnError,
   }

   public class ScriptHost
   {
      protected internal static readonly Logger logger;
      protected StringDict<ScriptFileAdmin> scripts;
      protected Assembly _compiledAssembly;
      protected CompilerParameters _cp;
      protected CompilerResults _cr;
      protected StringDict _refs;

      public ScriptHostFlags Flags { get; set; }
      public Assembly CompiledAssembly { get { return _compiledAssembly; } }
      public CompilerResults CompilerResults { get { return _cr; } }
      public CompilerParameters CompilerParameters { get { return _cp; } }

      static ScriptHost()
      {
         logger = Logs.CreateLogger("C# Scripts", "Scripthost");
      }

      public ScriptHost() : this(ScriptHostFlags.Default) { }
      public ScriptHost(ScriptHostFlags flags)
      {
         Flags = flags;
         Clear();
         ClearCompilerParms();
      }
      public void Clear()
      {
         scripts = new StringDict<ScriptFileAdmin>();
         Utils.FreeAndNil(ref _compiledAssembly);
         _cr = null;
      }

      public void ClearCompilerParms()
      {
         _cp = new CompilerParameters();
         _cp.CompilerOptions="/debug:pdbonly";// TreatWarningsAsErrors="false"
         _refs = new StringDict();
         AddReference("system.dll");
         AddReference("system.xml.dll");
         AddReference("system.linq.dll");
      }


      private void resolveAssemblies()
      {
         StringDict pathes = new StringDict();
         var domain = AppDomain.CurrentDomain;
         pathes[IOUtils.DelSlash(domain.BaseDirectory)] = null;
         String relPath = domain.RelativeSearchPath;
         if (relPath != null)
         {
            relPath = Path.Combine(domain.BaseDirectory, relPath);
            pathes[IOUtils.DelSlash(relPath)] = null;
         }
 
         var list = _cp.ReferencedAssemblies;
         for (int i = 0; i < list.Count; i++)
         {
            String dir = Path.GetDirectoryName(list[i]);
            if (String.IsNullOrEmpty(dir)) continue;
            pathes[dir] = null;
         }

         foreach (var kvp in pathes)
            resolveAssemblies(kvp.Key);
      }
      private void resolveAssemblies(String path)
      {
         var list = _cp.ReferencedAssemblies;
         for (int i = 0; i < list.Count; i++)
         {
            String fn = list[i];
            if (Path.GetFileName(fn) != fn) continue;

            String full = Path.Combine(path, fn);
            if (File.Exists(full))
            {
               list[i] = full;
               continue;
            }
            full += ".dll";
            if (File.Exists(full))
            {
               list[i] = full;
               continue;
            }
         }
      }

      public void AddReference(String loc)
      {
         if (String.IsNullOrEmpty(loc)) return;
         String fname = Path.GetFileName(loc);
         String fnameext = fname + ".dll";
         String name = Path.GetFileNameWithoutExtension(loc);

         if (_refs.ContainsKey(loc)) return;
         if (_refs.ContainsKey(fnameext)) return;
         if (_refs.ContainsKey(fname)) return;
         if (_refs.ContainsKey(name)) return;

         _cp.ReferencedAssemblies.Add(loc);
         _refs.OptAdd(loc, null);
         _refs.OptAdd(fnameext, null);
         _refs.OptAdd(fname, null);
         _refs.OptAdd(name, null);
      }
      public void AddReferenceFromType(Type t)
      {
         AddReference(t.Assembly);
      }
      public void AddReference(Assembly a)
      {
         //Check for dynamic assemblies: they have no location, which will cause an exception   
         if (a is _AssemblyBuilder) return;
         AddReference(a.Location);
      }

      public Object CreateObject(String typeName)
      {
         if (_compiledAssembly == null) throw new BMException("Script is not yet compiled.");
         return Objects.CreateObject(typeName);
      }

      public Object CreateObject(String typeName, params Object[] parms)
      {
         if (_compiledAssembly == null) throw new BMException("Script is not yet compiled.");
         return Objects.CreateObject(typeName, parms);
      }

      public void AddFile(String filename)
      {
         String fullName = Path.GetFullPath(filename);
         if (scripts.ContainsKey(fullName)) return;

         ScriptFileAdmin a = new ScriptFileAdmin(fullName);
         scripts.Add(a.FileName, a);

         foreach (var asm in a.References) AddReference(asm);
         foreach (var incl in a.Includes) AddFile (incl);
      }

      public CompilerResults Compile()
      {
         logger.Log("compile: flags={0}", Flags);
         if ((Flags & ScriptHostFlags.WarningAsError) != 0)
         {
            _cp.TreatWarningsAsErrors = true;
            _cp.CompilerOptions += " /warnaserror-:618";  //Obsolete
         }
         _cp.GenerateExecutable = false;

         CompilerResults cr;
         int i, n = scripts.Count;

         if (n == 0) throw new BMException("No scripts to compile");

         resolveAssemblies();
         if ((Flags & ScriptHostFlags.AddLoadedAsReference) != 0)
         {
            foreach (Assembly a in AppDomain.CurrentDomain.GetAssemblies()) AddReference (a);
         }

         String[] sources = new String[n];
         i = 0;
         foreach (KeyValuePair<String, ScriptFileAdmin> x in scripts)
            sources[i++] = x.Value.FileName;

         using (CSharpCodeProvider _cc = new CSharpCodeProvider())
         {
            _cr = cr = _cc.CompileAssemblyFromFile(_cp, sources);
         }
         _cr = cr;
         Utils.FreeAndNil(ref _compiledAssembly);

         //Exit if everything OK
         logger.Log("Compile rc={0}", cr.NativeCompilerReturnValue);
         if (cr.NativeCompilerReturnValue == 0)
         {
            _compiledAssembly = cr.CompiledAssembly;
            return cr;
         }

         logger.Log("compile result={0}. Loaded references: {1}", cr.NativeCompilerReturnValue, _cp.ReferencedAssemblies.Count);
         foreach (String x in _cp.ReferencedAssemblies)
            logger.Log("-- {0}", x);

         CompilerError firstErr = getFirstError(cr.Errors);
         dumpErrors(cr.Errors, (Flags & ScriptHostFlags.WarningAsError) == 0);
         if ((Flags & ScriptHostFlags.ExceptOnError) != 0)
         {
            throw new BMException("Script compilation failed.\nFirst error: {0}.\nSee '{1}' log for more details", firstErr, Path.GetFileNameWithoutExtension (logger.Name));
         }
         return cr;
      }

      protected static CompilerError getFirstError(CompilerErrorCollection errors)
      {
         CompilerError firstWarn = null;
         foreach (CompilerError err in errors)
         {
            if (!err.IsWarning) return err;
            firstWarn = err;
         }
         return firstWarn;
      }

      protected void dumpErrors (CompilerErrorCollection errors, bool errorsOnly)
      {
         bool somethingLogged = false;
         foreach (CompilerError err in errors)
         {
            if (err.IsWarning && errorsOnly) continue;
            dumpError(err, 5);
            somethingLogged = true;
         }
         if (!somethingLogged && errorsOnly)
            dumpErrors(errors, false);
      }
      private void dumpError(CompilerError err, int numLines)
      {
         logger.Log ();
         logger.Log (err.ToString());
         List<String> lines = null;
         try
         {
            lines = LoadLinesFromFile (err.FileName);
         }
         catch (Exception e)
         {
            logger.Log ("-- Cannot read lines: " + e.Message);
            return;
         }

         int errLine = err.Line;
         int from = errLine - numLines / 2;
         int to = errLine + (numLines+1) / 2;
         if (from < 0) from = 0;
         if (to > lines.Count) to = lines.Count;
         if (from >= to)
         {
            logger.Log (_LogType.ltError, "Cannot get line {0}. Source contains {1} lines.", errLine, lines.Count);
            return;
         }

         StringBuilder b = new StringBuilder();
         for (int i=from; i<to; i++)
         {
            b.AppendFormat ("[{0}] {1}\r\n", i, lines[i]);
         }
         logger.Log (b.ToString());
      }

      /// <summary>
      /// Does same as File.ReadAllText.
      /// </summary>
      /// <param name="fn">The file to be read</param>
      /// <returns></returns>
      public static List<String> LoadLinesFromFile(String fn)
      {
         Encoding encoding=null;
         return LoadLinesFromFile(fn, ref encoding);
      }
      /// <summary>
      /// Does same as File.ReadAllText.
      /// </summary>
      /// <param name="fn">The file to be read</param>
      /// <param name="encoding">used encoding</param>
      /// <returns></returns>
      public static List<String> LoadLinesFromFile(String fn, ref Encoding encoding)
      {
         var ret = new List<String>();
         FileStream strm = null;
         StreamReader r = null;
         try
         {
            strm = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (encoding != null)
               r = new StreamReader(strm, encoding);
            else
            {
               r = new StreamReader(strm, true);
               encoding = r.CurrentEncoding;
            }
            while (true)
            {
               String line = r.ReadLine();
               if (line==null) break;
               ret.Add (line);
            }
         }
         catch (Exception e)
         {
            Utils.Free(r);
            Utils.Free(strm);
            throw new BMException(e, "{0}\r\nFile={1}.", e.Message, fn);
         }
         Utils.Free(r);
         Utils.Free(strm);
         return ret;
      }


   }

   public class ScriptFileAdmin
   {
      public readonly String FileName;
      public readonly List<String> Lines;
      public readonly List<String> References;
      public readonly List<String> Includes;

      public ScriptFileAdmin(String file)
      {
         FileName = Path.GetFullPath(file);
         Lines = new List<string>();
         References = new List<string>();
         Includes = new List<string>();
         load(FileName);
      }

      private void load(String fn)
      {
         Regex refExpr = new Regex ("^//@ref=(.*)$", RegexOptions.IgnoreCase | RegexOptions.IgnoreCase); 
         Regex inclExpr = new Regex ("^//@incl=(.*)$", RegexOptions.IgnoreCase | RegexOptions.IgnoreCase); 
         var rdr = new StringReader (IOUtils.LoadFromFile(fn));
         String dir = Path.GetDirectoryName (fn);
         while (true)
         {
            String line = rdr.ReadLine();
            String val1;
            if (line==null) break;

            Lines.Add(line);
            if (line.Length < 5) continue;
            if (line[0] != '/') continue;
            if (refExpr.IsMatch (line))
            {
               val1 = refExpr.Replace (line, "$1");
               ScriptHost.logger.Log("handling REF: '{0}'", val1);
               if (!String.IsNullOrEmpty(val1)) References.Add(val1);
               continue;
            }
            if (inclExpr.IsMatch (line))
            {
               val1 = inclExpr.Replace (line, "$1");
               ScriptHost.logger.Log("handling INCL: '{0}'", val1);
               if (!String.IsNullOrEmpty(val1))
               {
                  Includes.Add(IOUtils.FindFileToRoot(dir, val1, FindToTootFlags.Except));
               }
               continue;
            }
         }
      }
   }
}
