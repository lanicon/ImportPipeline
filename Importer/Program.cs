using Bitmanager.Core;
using Bitmanager.ImportPipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Bitmanager.Importer
{
   public static class Program
   {

      /// <summary>
      /// The main entry point for the application.
      /// </summary>
      [STAThread]
      static int Main(String[] args)
      {
         if (args != null && args.Length > 0)
         {
            return runAsConsole(args);
         }

         Application.EnableVisualStyles();
         Application.SetCompatibleTextRenderingDefault(false);
         Application.Run(new Form1());
         return 0;
      }

      public static String[] Args;
      private static void logError(String fmt, params Object[] args)
      {
         String msg = String.Format(fmt, args);
         Logs.ErrorLog.Log(_LogType.ltError, msg);
         Console.WriteLine(msg);
      }

      private static int runAsConsole(String[] args)
      {
         try
         {
            bool b = Bitmanager.Core.ConsoleHelpers.AttachConsole();
            if (!b) b = Bitmanager.Core.ConsoleHelpers.AllocConsole();
         }
         catch { }

         try
         {
            Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
            var cmd = new CommandLineParms(args);
            String responseFile = cmd.NamedArgs.OptGetItem("resp");
            if (responseFile != null) {
               if (cmd.Args.Count != 0) goto WRITE_SYNTAX;
               cmd = new CommandLineParms(responseFile);
            }

            _ImportFlags flags = Invariant.ToEnum<_ImportFlags>(cmd.NamedArgs.OptGetItem("flags"), _ImportFlags.UseFlagsFromXml);
            int maxAdds = Invariant.ToInt32(cmd.NamedArgs.OptGetItem("maxadds"), -1);
            int maxEmits = Invariant.ToInt32(cmd.NamedArgs.OptGetItem("maxemits"), -1);
            if (cmd.Args.Count == 0) goto WRITE_SYNTAX;

            ImportEngine eng = new ImportEngine();
            eng.MaxAdds = maxAdds;
            eng.MaxEmits = maxEmits;
            String[] dsList = new String[cmd.Args.Count - 1];
            for (int i=1; i<cmd.Args.Count; i++)
               dsList[i-1] = cmd.Args[i];

            Logs.DebugLog.Log("4");
            eng.Load(cmd.Args[0]);
            if (flags != _ImportFlags.UseFlagsFromXml)
               eng.ImportFlags = flags;
            eng.Import(dsList);
            return 0;

            WRITE_SYNTAX:
            logError("Invalid commandline: {0}", Environment.CommandLine);
            logError("Syntax: <importxml file> [list of datasources] [/flags:<importflags>] [/maxadds:<number>] [/maxemits:<number>] [/$$xxxx$$:<value>");
            logError("    or: /resp:<responsefile> with 1 option per line");
            return 12;
         }
         catch (Exception e)
         {
            logError ("Error: {0}\r\nType: {1}\r\nStack:\r\n{2}", e.Message, e.GetType().FullName, e.StackTrace);
            return 12;
         }

      }
   }
}