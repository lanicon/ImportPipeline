using Bitmanager.Core;
using System;
using System.Collections.Generic;
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


      public CommandLineParms(String[] args)
      {
         NamedArgs = new StringDict();
         Args = new List<string>();
         if (args == null || args.Length == 0) return;

         Regex flagMatcher1 = new Regex(@"^\s*/([^\s]*)\s*[:=]\s*(.*)\s*$");
         Regex flagMatcher2 = new Regex(@"^\s*/([^\s]*)\s*$");
         for (int i = 0; i < args.Length; i++)
         {
            String s = args[i];
            bool ok = flagMatcher1.IsMatch(s);
            Match m = flagMatcher1.Match (s);
            if (m.Success)
            {
               NamedArgs[m.Groups[1].Value.ToLowerInvariant()] = m.Groups[2].Value;
               continue; 
            }
            ok = flagMatcher2.IsMatch(s);
            m = flagMatcher2.Match (s);
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
