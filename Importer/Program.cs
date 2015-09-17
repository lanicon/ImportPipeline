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
            if (cmd.NamedArgs.ContainsKey("?") || cmd.NamedArgs.ContainsKey("help")) goto WRITE_SYNTAX;

            String responseFile = cmd.NamedArgs.OptGetItem("resp");
            if (responseFile != null) {
               if (cmd.Args.Count != 0) goto WRITE_SYNTAX_ERR;
               cmd = new CommandLineParms(responseFile);
            }

            _ImportFlags flags = Invariant.ToEnum<_ImportFlags>(cmd.NamedArgs.OptGetItem("flags"), _ImportFlags.UseFlagsFromXml);
            int maxAdds = Invariant.ToInt32(cmd.NamedArgs.OptGetItem("maxadds"), -1);
            int maxEmits = Invariant.ToInt32(cmd.NamedArgs.OptGetItem("maxemits"), -1);
            if (cmd.Args.Count == 0) goto WRITE_SYNTAX_ERR;

            using (ImportEngine eng = new ImportEngine())
            {
               eng.MaxAdds = maxAdds;
               eng.MaxEmits = maxEmits;
               eng.ImportFlags = flags;
               String[] dsList = new String[cmd.Args.Count - 1];
               for (int i = 1; i < cmd.Args.Count; i++)
                  dsList[i - 1] = cmd.Args[i];

               eng.Load(cmd.Args[0]);
               eng.Import(dsList.Length == 0 ? null : dsList);
            }
            return 0;

            WRITE_SYNTAX_ERR:
            logError("Invalid commandline: {0}", Environment.CommandLine);
            WRITE_SYNTAX:
            logError("");
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