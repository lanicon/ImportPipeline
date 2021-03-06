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
using Bitmanager.Java;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class ProcessHost: NamedItem
   {
      public readonly ProcessHostSettings Settings;
      public ConsoleRunner Runner;

      public ProcessHost(XmlNode node): base (node)
      {
         Settings = new ProcessHostSettings(node);
      }
      public void Start()
      {
         if (Runner != null) return;

         ConsoleRunner tmp = new ConsoleRunner(Settings, Name);
         tmp.Start();
         Runner = tmp;
      }
   }

   public class ProcessHostCollection : NamedAdminCollection<ProcessHost>
   {
      private Logger logger;
      private bool initDone;
      public ProcessHostCollection(ImportEngine engine, XmlNode collNode)
         : base(collNode, "process", (n) => new ProcessHost (n), false)
      {
         logger = engine.ImportLog.Clone("processHost");
      }
      
      [DllImport("kernel32.dll")]
      static extern bool SetConsoleCtrlHandler(ConsoleHelpers.ConsoleCtrlDelegate HandlerRoutine,  bool Add);
      public void EnsureStarted(String name)
      {
         if (!initDone)
         {
            try
            {
               Console.CancelKeyPress += Console_CancelKeyPress;
            }
            catch { }

            //ConsoleHelpers.AllocConsole();
            //SetConsoleCtrlHandler(null, true);
            //ConsoleHelpers.AddConsoleCtrlHandler(ctrlHandler);    
            initDone = true;
         }
         GetByName (name).Start();
      }

      void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
      {
         e.Cancel = true;
      }

      private static bool ctrlHandler(CtrlTypes CtrlType)
      {
         return true;
      }

      public void StopAll()
      {
         if (Count == 0) return;

         //Collect all runners and take ownership
         List<ConsoleRunner> runners = new List<ConsoleRunner>();
         for (int i = 0; i < Count; i++)
         {
            ConsoleRunner runner = this[i].Runner;
            if (runner == null) continue;
            runners.Add(runner);
            this[i].Runner = null;
         }
         if (runners.Count == 0) return;

         DateTime limit = DateTime.UtcNow.AddSeconds(30);
         int stoppedNormal = 0;
         int stoppedError = 0;
         for (int phase = 0; phase < 3; phase++)
         {
            bool atLeastOneWantedToWait = false;
            for (int i = 0; i < runners.Count; i++)
            {
               ConsoleRunner runner = runners[i];
               try
               {
                  //logger.Log("StopAll -- phase={0} runner={1}.", phase, runner.Name);
                  bool waitNeeded = false;
                  switch (phase)
                  {
                     case 0: waitNeeded = runner.Stop_Initiate(); break;
                     case 1: waitNeeded = runner.Stop_CtrlC(); break;
                     case 2: waitNeeded = runner.Stop_Kill(); break;
                  }
                  if (waitNeeded) atLeastOneWantedToWait = true;
               }
               catch (Exception e)
               {
                  logger.Log("StopAll failed in phase {0} for runner {1}.", phase, runner.Name);
                  logger.Log(e);
               }
            }
            //logger.Log("StopAll -- atLeastOneWantedToWait={0}.", atLeastOneWantedToWait);
            if (!atLeastOneWantedToWait) continue;
            if (waitForAllExit(runners, limit)) break;
            limit = DateTime.UtcNow.AddSeconds(30);
         }

         //Check if there were any errors
         for (int i = 0; i < runners.Count; i++)
         {
            ConsoleRunner runner = runners[i];
            runner.CheckStoppedAndDispose();
            if (runner.ErrorsDuringExit)
               stoppedError++;
            else
               stoppedNormal++;
         }

         if (stoppedError == 0)
         {
            logger.Log(_LogType.ltInfo, "All processes ({0}) stopped correctly...", stoppedNormal);
            return;
         }
         logger.Log(_LogType.ltError, "Stopped {0} processes. {1} of them stopped correctly, {2} failed.", stoppedNormal + stoppedError, stoppedNormal, stoppedError);
      }

      protected bool waitForAllExit(List<ConsoleRunner> runners, DateTime limit)
      {
         bool allExited = true;
         for (int i = 0; i < runners.Count; i++)
         {
            var runner = this[i].Runner;
            if (runner == null) continue;
            int msLeft = (int)limit.Subtract(DateTime.UtcNow).TotalMilliseconds;
            if (msLeft < 0) msLeft = 0;
            if (!runner.WaitForExit(msLeft)) allExited = false;
            //logger.Log("StopAll -- waited for exit {0}. code={1}", i, runner.process==null ? -1 : runner.process.ExitCode);
         }
         //logger.Log("StopAll -- allExited={0}", allExited);
         return allExited;
      }
   }
}
