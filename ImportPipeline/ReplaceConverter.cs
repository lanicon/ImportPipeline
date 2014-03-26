using Bitmanager.Core;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public enum ReplacerFlags
   {
      EvaluateAll = 1,
      NoMatchReturnNull = 2,
      NoMatchReturnOriginal = 4,
   }

   public class ReplaceConverter : Converter
   {
      protected List<ReplacerElt> replacers;
      protected ReplacerFlags flags;
      protected StringDict missed;
      protected int maxMissed;

      public ReplaceConverter(XmlNode node) : base (node)
      {
         ReplacerFlags def = ReplacerFlags.NoMatchReturnOriginal;
         maxMissed = XmlUtils.OptReadInt(node, "@dumpmissed", -1);
         if (maxMissed > 0)
         {
            missed = new StringDict();
            def = ReplacerFlags.NoMatchReturnNull;
         }
         flags = XmlUtils.OptReadEnum(node, "@flags", def);
         replacers = new List<ReplacerElt>();
         XmlNodeList list = node.SelectNodes("replace");
         for (int i = 0; i < list.Count; i++)
         {
            var r = new ReplacerElt(list[i]);
            if (r == null) continue;
            replacers.Add(r);
         }
      }

      public override object ConvertScalar(PipelineContext ctx, object obj)
      {
         if (obj == null) return null;
         String arg = obj.ToString();
         if (TryReplace(ref arg)) return arg;

         return ((flags & ReplacerFlags.NoMatchReturnNull) != 0) ? null : arg;
      }
      public override void DumpMissed(PipelineContext ctx)
      {
         DumpMissed(ctx.MissedLog);
         missed = null;
      }


      public String Replace(String val)
      {
         String ret = val;
         if (TryReplace(ref ret)) return ret;

         return ((flags & ReplacerFlags.NoMatchReturnNull) != 0) ? null : val;
      }

      public bool TryReplace(ref String val)
      {
         if (String.IsNullOrEmpty(val)) return false;

         bool replaced = false;
         for (int i = 0; i < replacers.Count; i++)
         {
            if (!replacers[i].TryReplace(ref val)) continue;

            replaced = true;
            if ((flags & ReplacerFlags.EvaluateAll) == 0) return true;
         }

         if (replaced) return true;

         //Optional administrate missed.
         if (!String.IsNullOrEmpty(val) && missed != null && missed.Count < maxMissed)
            missed.OptAdd(val, null);
         return false;
      }

      public int MissedCount { get { return missed == null ? 0 : missed.Count; } }

      public void DumpMissed(Logger logger, String prefix = "-- ")
      {
         if (missed==null) return;
         logger.Log ("Missed '{0}' conversions: {1}", Name, missed.Count);
         foreach (var kvp in missed)
         {
            logger.Log(prefix + kvp.Key);
         }
      }



   }
   public class ReplacerElt
   {
      private Regex regex;
      private string replacement;
      private string value;

      public ReplacerElt(XmlNode node)
      {
         replacement = XmlUtils.ReadStrRaw(node, "@repl", _XmlRawMode.EmptyToNull);
         String tmp = XmlUtils.OptReadStr(node, "@expr", null);
         if (tmp != null)
            regex = new Regex(tmp, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
         else
            value = XmlUtils.ReadStr(node, "@value");
      }

      public bool TryReplace(ref String val)
      {
         if (val == null) return false;

         if (regex != null)
         {
            if (!regex.IsMatch(val)) return false;
            val = (replacement == null) ? null : regex.Replace(val, replacement);
            return true;
         }

         if (!String.Equals(val, value, StringComparison.InvariantCultureIgnoreCase)) return false;
         val = replacement;
         return true;
      }

      public override string ToString()
      {
         if (regex == null)
            return String.Format("Replacer (str={0})=>{1})", value, replacement);

         return String.Format("Replacer (regex={0})=>{1})", regex, replacement);
      }
   }

}
