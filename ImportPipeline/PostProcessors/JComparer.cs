using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{

   /// <summary>
   /// Comparer object. Compares one or more keys from two JObject's
   /// </summary>
   public class JComparer : IComparer<JObject>
   {
      protected static readonly int NULLHASH = 0x12345678;
      public virtual int Compare (JObject a, JObject b)
      {
         return 0;
      }

      public static JComparer Create(List<KeyAndType> keyAndTypes)
      {
         if (keyAndTypes == null || keyAndTypes.Count == 0) return new JComparer();
         switch (keyAndTypes.Count)
         {
            case 1: return Create(keyAndTypes[0]);
            case 2: return new Comparer2(Create(keyAndTypes[0]), Create(keyAndTypes[1]));
            default:
               var comparers = new JComparer[keyAndTypes.Count];
               for (int i = 0; i < keyAndTypes.Count; i++)
                  comparers[i] = Create(keyAndTypes[i]);
               return new ComparerN(comparers);
         }
      }

      private static JComparer Create(KeyAndType keyAndType)
      {
         switch (keyAndType.Type & (CompareType.String | CompareType.Int | CompareType.Long | CompareType.Double | CompareType.Date))
         {
            case CompareType.String: return new Comparer1Str(keyAndType);
            case CompareType.Int: return new Comparer1Int(keyAndType);
            case CompareType.Long: return new Comparer1Long(keyAndType);
            case CompareType.Double: return new Comparer1Dbl(keyAndType);
            case CompareType.Date: return new Comparer1Date(keyAndType);
            default: keyAndType.Type.ThrowUnexpected(); return null;
         }
      }

      protected static JToken normalizeToken(JToken tk)
      {
         if (tk == null) goto EXIT_RTN;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: return null;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) return null;
               break;
         }
      EXIT_RTN:
         return tk;
      }
   }


   //////////////////////////////////////////////////////////////////////////////////////
   // Actual implementations
   //////////////////////////////////////////////////////////////////////////////////////

   abstract class Comparer1Base : JComparer
   {
      protected readonly JPath path;
      protected readonly bool reverse;
      public Comparer1Base(KeyAndType keyAndType)
      {
         this.path = keyAndType.Key;
         this.reverse = (keyAndType.Type & CompareType.Descending) != 0;
      }
   }

   class Comparer1Str : Comparer1Base
   {
      private readonly StringComparison comparison;
      public Comparer1Str(KeyAndType keyAndType): base(keyAndType)
      {
         this.comparison = (keyAndType.Type & CompareType.CaseInsensitive) != 0 ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
      }
      public override int Compare(JObject a, JObject b)
      {
         int rc = String.Compare((String)normalizeToken(path.Evaluate(a, JEvaluateFlags.NoExceptMissing)), 
                                 (String)normalizeToken(path.Evaluate(b, JEvaluateFlags.NoExceptMissing)), comparison);
         return reverse ? -rc : rc;
      }
   }

   class Comparer1Int : Comparer1Base
   {
      static readonly Comparer<int> cmp = Comparer<int>.Default;
      public Comparer1Int(KeyAndType keyAndType)
         : base(keyAndType)
      {
      }
      public override int Compare(JObject a, JObject b)
      {
         JToken ta = normalizeToken(path.Evaluate(a, JEvaluateFlags.NoExceptMissing));
         JToken tb = normalizeToken(path.Evaluate(b, JEvaluateFlags.NoExceptMissing));
         int rc;
         if (ta==null)
            rc = tb==null ? 0 : -1;
         else if (tb==null)
            rc = 1;
         else
            rc = cmp.Compare ((int)ta, (int)tb);
         return reverse ? -rc : rc;
      }
   }


   class Comparer1Long : Comparer1Base
   {
      static readonly Comparer<long> cmp = Comparer<long>.Default;
      public Comparer1Long(KeyAndType keyAndType)
         : base(keyAndType)
      {
      }
      public override int Compare(JObject a, JObject b)
      {
         JToken ta = normalizeToken(path.Evaluate(a, JEvaluateFlags.NoExceptMissing));
         JToken tb = normalizeToken(path.Evaluate(b, JEvaluateFlags.NoExceptMissing));
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((long)ta, (long)tb);
         return reverse ? -rc : rc;
      }
   }


   class Comparer1Dbl : Comparer1Base
   {
      static readonly Comparer<double> cmp = Comparer<double>.Default;
      public Comparer1Dbl(KeyAndType keyAndType)
         : base(keyAndType)
      {
      }
      public override int Compare(JObject a, JObject b)
      {
         JToken ta = normalizeToken(path.Evaluate(a, JEvaluateFlags.NoExceptMissing));
         JToken tb = normalizeToken(path.Evaluate(b, JEvaluateFlags.NoExceptMissing));
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((double)ta, (double)tb);
         return reverse ? -rc : rc;
      }
   }


   class Comparer1Date : Comparer1Base
   {
      static readonly Comparer<DateTime> cmp = Comparer<DateTime>.Default;
      public Comparer1Date(KeyAndType keyAndType)
         : base(keyAndType)
      {
      }
      public override int Compare(JObject a, JObject b)
      {
         JToken ta = normalizeToken(path.Evaluate(a, JEvaluateFlags.NoExceptMissing));
         JToken tb = normalizeToken(path.Evaluate(b, JEvaluateFlags.NoExceptMissing));
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((DateTime)ta, (DateTime)tb);
         return reverse ? -rc : rc;
      }
   }

   class Comparer2 : JComparer
   {
      private readonly JComparer cmp1, cmp2;
      public Comparer2(JComparer cmp1, JComparer cmp2)
      {
         this.cmp1 = cmp1;
         this.cmp2 = cmp2;
      }
      public override int Compare(JObject a, JObject b)
      {
         int rc = cmp1.Compare(a, b);
         return rc == 0 ? cmp2.Compare(a, b) : rc;
      }
   }
   class ComparerN : JComparer
   {
      private readonly JComparer[] cmps;
      public ComparerN(JComparer[] cmps)
      {
         this.cmps = cmps;
      }
      public override int Compare(JObject a, JObject b)
      {
         for (int i = 0; i < cmps.Length; i++)
         {
            int rc = cmps[i].Compare(a, b);
            if (rc != 0) return rc;
         }
         return 0;
      }
   }
}
