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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.IO;
using System.Globalization;
using Bitmanager.Elastic;
using Bitmanager.IO;
using Bitmanager.Java;

namespace Bitmanager.ImportPipeline
{
   public class CommandEndpoint : Endpoint
   {
      public readonly Command[] Commands;

      public CommandEndpoint(ImportEngine engine, XmlNode node)
         : base(engine, node, ActiveMode.Lazy | ActiveMode.Local)
      {
         XmlNodeList chlds = node.ChildNodes;// node.SelectMandatoryNodes("*");
         var cmds = new List<Command>(chlds.Count);
         for (int i = 0; i < chlds.Count; i++)
         {
            XmlNode n = chlds[i];
            String name = n.Name;
            Command cmd;
            switch (name)
            {
               default: continue;
               case "command":
               case "exec": cmd = new ExecCommand(engine, n); break;
               case "delete":
               case "del": cmd = new DeleteCommand(engine, n); break;
               case "copy": cmd = new CopyCommand(engine, n); break;
               case "move": cmd = new MoveCommand(engine, n); break;
            }
            cmds.Add (cmd);
         }
         Commands = cmds.ToArray();
         if (cmds.Count == 0)
            engine.ImportLog.Log("Endpoint [{0}] has no commands. Resulting in a no-op.", Name);
      }


      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string name, bool mustExcept = true)
      {
         return new CommandDataEndpoint(this);
      }



      public class CommandDataEndpoint : JsonEndpointBase<CommandEndpoint>
      {
         Command[] Commands; 
         public CommandDataEndpoint(CommandEndpoint endpoint)
            : base(endpoint)
         {
            Commands = endpoint.Commands;
         }

         public override void Add(PipelineContext ctx)
         {
            if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0)
            {
               ctx.DebugLog.Log("Add: accumulator.Count={0}", accumulator.Count);
            }
            if (accumulator.Count == 0) return;
            OptLogAdd();

            foreach (var cmd in Commands)
            {
               cmd.Execute(ctx, accumulator);
            }
            Clear();
         }
      }


      public abstract class Command
      {
         protected String WorkingDir;
         protected String[] fields;
         protected Object[] formatParms;
         protected int numErrors;
         protected bool rawTokens;
         protected bool errorsAsWarning;
         public abstract void Execute(PipelineContext ctx, JObject obj);

         protected Command(ImportEngine eng, XmlNode node)
         {
            WorkingDir = node.ReadStr("@curdir", null);
            WorkingDir = eng.Xml.CombinePath(WorkingDir);
            rawTokens = node.ReadBool("@rawtokens", false);
            fields = node.ReadStr("@arguments", null).SplitStandard();
            if (fields != null && fields.Length > 0)
               formatParms = new Object[fields.Length];
            else
               fields = null;
         }

         protected Object[] fillParams(PipelineContext ctx, JObject obj)
         {
            if (fields != null)
            {
               for (int i = 0; i < fields.Length; i++)
               {
                  JToken tk = obj[fields[i]];
                  if (tk == null)
                  {
                     formatParms[i] = null;
                     continue;
                  }
                  formatParms[i] = rawTokens ? tk : tk.ToNative();
               }
            }
            return formatParms;
         }

         protected void issueError(PipelineContext ctx, String msg)
         {
            numErrors++;
            if (errorsAsWarning)
               ctx.ImportLog.Log(_LogType.ltError, msg);
            else
               throw new BMException(msg);
         }
      }

      public class ExecCommand : Command
      {
         protected readonly String cmd;
         protected readonly ProcessHostSettings settings;

         public ExecCommand(ImportEngine eng, XmlNode node)
            : base(eng, node)
         {
            cmd = node.ReadStr("@cmd");
            if (!cmd.StartsWith("cmd "))
               cmd = "cmd /c " + cmd;
            settings = new ProcessHostSettings();
            settings.LogName = node.ReadStr("@log", settings.LogName);
            settings.LogName = node.ReadStr("@errorlog", settings.LogName);
            settings.ExeName = "cmd.exe";
         }

         public override void Execute(PipelineContext ctx, JObject obj)
         {
            String actualCmd = Invariant.Format(cmd, fillParams(ctx, obj));
            settings.Arguments = Invariant.Format(cmd, fillParams(ctx, obj));
            settings.WorkingDir = WorkingDir;

            using (ConsoleRunner runner = new ConsoleRunner(settings, null))
            {
               runner.Start();
               if (!runner.WaitForExit(10000))
                  runner.Stop_CtrlC();
               if (runner.ExitCode != 0)
               {
                  issueError(ctx, String.Format("cmd.exe {0}\nEnded with rc={1}.", actualCmd, runner.ExitCode));
               }
            }
         }
      }

      public class DeleteCommand : Command
      {
         protected readonly String src;

         public DeleteCommand(ImportEngine eng, XmlNode node)
            : base(eng, node)
         {
            src = node.ReadStr("@file");
         }

         public override void Execute(PipelineContext ctx, JObject obj)
         {
            String actualSrc = Invariant.Format(src, fillParams(ctx, obj));
            if (WorkingDir != null)
            {
               actualSrc = Path.Combine(WorkingDir, actualSrc);
            }

            try
            {
               File.Delete(actualSrc);
            }
            catch (Exception err)
            {
               base.issueError(ctx, err.Message);
            }
         }
      }

      public class CopyCommand : Command
      {
         protected readonly String src;
         protected readonly String dst;
         protected readonly String dstDir;
         protected bool overWrite;

         public CopyCommand(ImportEngine eng, XmlNode node)
            : base(eng, node)
         {
            overWrite = node.ReadBool("@overwrite", true);
            src = node.ReadStr("@src");
            dst = node.ReadStr("@dst", null);
            dstDir = node.ReadStr("@dstdir", null);
            if (dstDir == null && dst == null) throw new BMNodeException(node, "Missing value for either @dst or @dstdir.");
            if (dstDir != null && dst != null) throw new BMNodeException(node, "Duplicate value: @dst and @dstdir are mutually exclusive.");
         }

         public override void Execute(PipelineContext ctx, JObject obj)
         {
            var parms = fillParams(ctx, obj);
            String actualSrc = Invariant.Format(src, parms);
            String actualDst;
            if (dst != null)
               actualDst = Invariant.Format(dst, parms);
            else
            {
               String fn = Path.GetFileName(actualSrc);
               actualDst = Path.Combine(Invariant.Format(dstDir, parms), fn);
            }

            if (WorkingDir != null)
            {
               actualSrc = Path.Combine(WorkingDir, actualSrc);
               actualDst = Path.Combine(WorkingDir, actualDst);
            }

            try
            {
               handleFile(actualSrc, actualDst);
            }
            catch (Exception err)
            {
               base.issueError(ctx, err.Message);
            }
         }

         protected virtual void handleFile(String src, String dst)
         {
            File.Copy(src, dst, overWrite);
         }
      }

      public class MoveCommand : CopyCommand
      {
         public MoveCommand(ImportEngine eng, XmlNode node)
            : base(eng, node)
         {
         }

         protected override void handleFile(String src, String dst)
         {
            if (File.Exists(dst) && overWrite)
               File.Delete(dst);
            File.Move(src, dst);
         }
      }

   }


}
