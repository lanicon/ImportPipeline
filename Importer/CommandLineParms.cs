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
