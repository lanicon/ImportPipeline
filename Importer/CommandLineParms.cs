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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Bitmanager.Importer
{
   public class CommandLineParms
   {
      public readonly StringDict NamedArgs;
      public readonly List<String> Args;

      private CommandLineParms()
      {
         NamedArgs = new StringDict();
         Args = new List<string>();
      }
      public CommandLineParms(String[] args): this()
      {
         parse(args);
      }

      public CommandLineParms(String responseFile): this()
      {
         List<String> lines = new List<string>();
         using (var rdr = File.OpenText(responseFile))
         {
            while (true)
            {
               String line = rdr.ReadLine();
               if (line == null) break;
               if (line.Length == 0) continue;
               lines.Add(line);
            }
         }
         parse(lines.ToArray());
      }

      private void parse(String[] args)
      {
         if (args == null || args.Length == 0) return;

         Regex flagMatcher1 = new Regex(@"^\s*/([^\s]*)\s*[:=]\s*(.*)\s*$");
         Regex flagMatcher2 = new Regex(@"^\s*/([^\s]*)\s*$");
         for (int i = 0; i < args.Length; i++)
         {
            String s = args[i];
            bool ok = flagMatcher1.IsMatch(s);
            Match m = flagMatcher1.Match(s);
            if (m.Success)
            {
               NamedArgs[m.Groups[1].Value.ToLowerInvariant()] = m.Groups[2].Value;
               continue;
            }
            ok = flagMatcher2.IsMatch(s);
            m = flagMatcher2.Match(s);
            if (m.Success)
            {
               NamedArgs[m.Groups[1].Value.ToLowerInvariant()] = null;
               continue;
            }
            Args.Add(s);
         }
      }

   }
}
