using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class SpecialCharsReplacer
   {
      public static char Replace(char x)
      {
         int ch = (int) x;
         if (ch < from || ch >= until) return x;
         return replacements[ch-from];
      }


      static char[] replacements;
      static int from, until;
      static SpecialCharsReplacer()
      {
         _Helper hlp = new _Helper();
         hlp.addReplacement(0x2000, 0x200B, ' ');
         hlp.addReplacement(0x00A0, ' ');
         hlp.addReplacement(0x202f, ' ');
         hlp.addReplacement(0x2060, ' ');
         hlp.addReplacement(0x00AB, '\"');
         hlp.addReplacement(0x300A, '\"');
         hlp.addReplacement(0x226A, '\"');
         hlp.addReplacement(0x00BB, '\"');
         hlp.addReplacement(0x226B, '\"');
         hlp.addReplacement(0x300B, '\"');
         hlp.addReplacement(0x2010, 0x2015, '-');
         hlp.addReplacement(0x2016, '|');
         hlp.addReplacement(0x2017, '_');
         hlp.addReplacement(0x2018, 0x201B, '\'');
         hlp.addReplacement(0x201C, 0x201F, '\"');
         hlp.addReplacement(0x2039, '<');
         hlp.addReplacement(0x203A, '>');
         hlp.addReplacement(0x203C, '!');
         hlp.addReplacement(0x2043, '-');
         hlp.addReplacement(0x2044, '/');
         hlp.addReplacement(0x2045, '!');
         hlp.addReplacement(0x203C, '!');
         hlp.addReplacement(0x203C, '!');
         hlp.toSpecialCharsReplacer();
      }


      private class _Helper
      {
         List<int> from = new List<int>();
         List<int> to = new List<int>();
         int lo = 0x10000;
         int hi = 0;

         public void addReplacement(int from, int to, int dst)
         {
            for (int i = from; i <= to; i++) addReplacement(i, dst);
         }
         public void addReplacement(int src, int dst)
         {
            if (src < lo) lo = src;
            if (src > hi) hi = src;
            from.Add((char)src);
            to.Add((char)dst);
         }

         public void toSpecialCharsReplacer()
         {
            char[] chars = new char[1 + hi - lo];
            for (int i = 0; i < from.Count; i++)
            {
               chars[from[i] - lo] = (char)to[i];
            }
            SpecialCharsReplacer.replacements = chars;
            SpecialCharsReplacer.from = lo;
            SpecialCharsReplacer.until = hi;
         }


      }
   }
}
