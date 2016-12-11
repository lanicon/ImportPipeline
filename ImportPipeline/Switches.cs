using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public class Switches
   {
      [Flags]
      enum SwitchBits { Off = 0, On = 1, Asked = 4 };
      static readonly char[] SEP = {' ', ',', ';', '|'};
      private Dictionary<String, SwitchBits> _switches;
      private Dictionary<String, SwitchBits> _asked;
      public Switches(String switches)
      {
         if (!String.IsNullOrEmpty(switches))
         {
            String[] arr = switches.Split(SEP);
            _switches = new Dictionary<string, SwitchBits>(arr.Length);
            for (int i=0; i<arr.Length; i++)
            {
               if (String.IsNullOrEmpty(arr[i])) continue;
               String k = arr[i].ToLowerInvariant();
               SwitchBits value;
               if (k[0] == '-')
               {
                  k = k.Substring(1);
                  value = SwitchBits.Off;
               }
               else
                  value = SwitchBits.On;
               _switches[k] = value;
            }
         }
      }

      /// <summary>
      /// Returns false if not specified or 'off'
      /// </summary>
      public bool IsOn(String k)
      {
         if (String.IsNullOrEmpty(k) || _switches == null) return false;
         k = k.ToLowerInvariant();
         if (_asked == null) _asked = new Dictionary<string, SwitchBits>();
         _asked[k] = 0;
         SwitchBits value;
         if (!_switches.TryGetValue(k, out value)) return false;
         value |= SwitchBits.Asked;
         _switches[k] = value;
         return (value & SwitchBits.On) != 0;
      }
      public bool IsOn(params String[] k)
      {
         if (k == null || k.Length == 0) return false;
         bool ret = false;
         for (int i = 0; i < k.Length; i++)
            ret |= IsOn(k[i]);
         return ret;
      }

      /// <summary>
      /// Returns false if not specified or 'on'
      /// </summary>
      public bool IsOff(String k)
      {
         if (String.IsNullOrEmpty(k) || _switches == null) return false;
         k = k.ToLowerInvariant();
         if (_asked == null) _asked = new Dictionary<string, SwitchBits>();
         _asked[k] = 0;
         SwitchBits value;
         if (!_switches.TryGetValue(k, out value)) return false;
         value |= SwitchBits.Asked;
         _switches[k] = value;
         return (value & SwitchBits.On) == 0;
      }
      public bool IsOff(params String[] k)
      {
         if (k == null || k.Length == 0) return true;
         bool ret = true;
         for (int i = 0; i < k.Length; i++)
            ret &= IsOff(k[i]);
         return ret;
      }

      public String GeUnknownSwitches()
      {
         int cnt;
         return dumpSwitches(_switches, out cnt);
      }
      public String GeUnknownSwitches(out int cnt)
      {
         return dumpSwitches(_switches, out cnt);
      }

      public String GeAskedSwitches()
      {
         int cnt;
         return dumpSwitches(_asked, out cnt);
      }
      public String GeAskedSwitches(out int cnt)
      {
         return dumpSwitches(_asked, out cnt);
      }


      private static String dumpSwitches(Dictionary<String, SwitchBits> dict, out int cnt)
      {
         cnt = 0;
         if (dict == null) return null;
         int tot = 0;
         List<String> tmp = null;
         foreach (var kvp in dict)
         {
            if ((kvp.Value & SwitchBits.Asked) != 0) continue;
            if (tmp == null)
               tmp = new List<string>();
            tmp.Add(kvp.Key);
            ++tot;
         }
         cnt = tot;
         if (tmp==null) return null;
         tmp.Sort();
         return String.Join(", ", tmp);
      }

   }
}
