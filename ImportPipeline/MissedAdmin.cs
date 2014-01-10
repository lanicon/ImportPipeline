using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class MissedAdmin
   {
      StringDict<bool> dict;

      public MissedAdmin()
      {
      }

      public void AddMissed(String x, bool touched= false)
      {
         if (x == null) return;

         x = x.ToLowerInvariant();
         if (dict == null)
         {
            dict = new StringDict<bool>();
            dict.Add(x, touched);
            return;
         }
         if (dict.ContainsKey (x)) return;
         dict.Add(x, touched);
      }

      public void Combine(MissedAdmin other)
      {
         if (other.dict == null) return;
         foreach (var kvp in other.dict)
         {
            AddMissed(kvp.Key, kvp.Value);
         }
      }
   }
}
