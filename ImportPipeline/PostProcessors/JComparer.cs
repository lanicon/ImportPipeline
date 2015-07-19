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
   /// Comparer object. Compares one or more keys from two JObject's.
   /// Also computes hashcode
   /// </summary>
   public class JComparer : IComparer<JObject>
   {
      protected static readonly int NULLHASH = 0; //x12345678; By using NULLHASH=0, all version of the hashers (1, 2, N) are more consistent...
      public virtual int Compare(JObject a, JObject b)
      {
         return 0;
      }
      public virtual int GetHash(JObject obj)
      {
         return 0;
      }
      public virtual int GetHash(JObject obj, out int nullIndex)
      {
         nullIndex = -1;
         return 0;
      }
      public virtual JToken[] GetKeys(JObject obj)
      {
         return new JToken[0];
      }
      public virtual int CompareKey(JToken a, JToken b)
      {
         return 0;
      }
      public virtual int CompareKeys(JToken[] a, JToken[] b)
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
               var comparers = new Comparer1Base[keyAndTypes.Count];
               for (int i = 0; i < keyAndTypes.Count; i++)
                  comparers[i] = Create(keyAndTypes[i]);
               return new ComparerN(comparers);
         }
      }

      private static Comparer1Base Create(KeyAndType keyAndType)
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

      internal virtual JToken _GetKey(JObject obj)
      {
         return normalizeToken(path.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
      }

      public override JToken[] GetKeys(JObject obj)
      {
         return new JToken[1] { _GetKey(obj) };
      }
      public override int CompareKeys(JToken[] a, JToken[] b)
      {
         if (a.Length < 1 || b.Length < 1) throw new BMException("Both key arrays should have minimal length 1. Lenght a/b is {0}/{1}.", a.Length, b.Length);
         return CompareKey(a[0], b[0]);
      }

   }

   class Comparer1Str : Comparer1Base
   {
      private readonly StringComparer stringComparer;
      private readonly StringComparison comparison;
      public Comparer1Str(KeyAndType keyAndType): base(keyAndType)
      {
         this.comparison = (keyAndType.Type & CompareType.CaseInsensitive) != 0 ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
         this.stringComparer = (keyAndType.Type & CompareType.CaseInsensitive) != 0 ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
      }

      public override int Compare(JObject a, JObject b)
      {
         int rc = String.Compare((String)normalizeToken(path.Evaluate(a, JEvaluateFlags.NoExceptMissing)), 
                                 (String)normalizeToken(path.Evaluate(b, JEvaluateFlags.NoExceptMissing)), comparison);
         return reverse ? -rc : rc;
      }

      public override int CompareKey(JToken a, JToken b)
      {
         int rc = String.Compare((String)normalizeToken(a),
                                 (String)normalizeToken(b), comparison);
         return reverse ? -rc : rc;
      }

      public override int GetHash(JObject obj)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         return stringComparer.GetHashCode((String)tk);

      RET_NULL:
         return NULLHASH;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         nullIndex = -1;
         return stringComparer.GetHashCode((String)tk);

      RET_NULL:
         nullIndex = 0;
         return NULLHASH;
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

      public override int CompareKey(JToken a, JToken b)
      {
         JToken ta = normalizeToken(a);
         JToken tb = normalizeToken(b);
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((int)ta, (int)tb);
         return reverse ? -rc : rc;
      }

      public override int GetHash(JObject obj)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         return ((int)tk);

      RET_NULL:
         return NULLHASH;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         nullIndex = -1;
         return ((int)tk);

      RET_NULL:
         nullIndex = 0;
         return NULLHASH;
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

      public override int CompareKey(JToken a, JToken b)
      {
         JToken ta = normalizeToken(a);
         JToken tb = normalizeToken(b);
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((long)ta, (long)tb);
         return reverse ? -rc : rc;
      }

      public override int GetHash(JObject obj)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         long tmp = (long)tk;
         return (int)tmp ^ (int)(tmp >> 32);

      RET_NULL:
         return NULLHASH;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         nullIndex = -1;
         long tmp = (long)tk;
         return (int)tmp ^ (int)(tmp >> 32);

      RET_NULL:
         nullIndex = 0;
         return NULLHASH;
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

      public override int CompareKey(JToken a, JToken b)
      {
         JToken ta = normalizeToken(a);
         JToken tb = normalizeToken(b);
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((double)ta, (double)tb);
         return reverse ? -rc : rc;
      }

      public override int GetHash(JObject obj)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         double d = (double)tk;
         return d.GetHashCode();

      RET_NULL:
         return NULLHASH;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         nullIndex = -1;
         double d = (double)tk;
         return d.GetHashCode();

      RET_NULL:
         nullIndex = 0;
         return NULLHASH;
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

      public override int CompareKey(JToken a, JToken b)
      {
         JToken ta = normalizeToken(a);
         JToken tb = normalizeToken(b);
         int rc;
         if (ta == null)
            rc = tb == null ? 0 : -1;
         else if (tb == null)
            rc = 1;
         else
            rc = cmp.Compare((DateTime)ta, (DateTime)tb);
         return reverse ? -rc : rc;
      }

      public override int GetHash(JObject obj)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         long tmp = ((DateTime)tk).Ticks;
         return (int)tmp ^ (int)(tmp >> 32);

      RET_NULL:
         return NULLHASH;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto RET_NULL;
         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto RET_NULL;
            case JTokenType.String:
               var s = (String)tk;
               if (s == null || s.Length == 0) goto RET_NULL;
               break;
         }
         nullIndex = -1;
         long tmp = ((DateTime)tk).Ticks;
         return (int)tmp ^ (int)(tmp >> 32);

      RET_NULL:
         nullIndex = 0;
         return NULLHASH;
      }
   }

   class Comparer2 : JComparer
   {
      private readonly Comparer1Base cmp1, cmp2;
      public Comparer2(Comparer1Base cmp1, Comparer1Base cmp2)
      {
         this.cmp1 = cmp1;
         this.cmp2 = cmp2;
      }
      public override int Compare(JObject a, JObject b)
      {
         int rc = cmp1.Compare(a, b);
         return rc == 0 ? cmp2.Compare(a, b) : rc;
      }

      public override JToken[] GetKeys(JObject obj)
      {
         return new JToken[2] {cmp1._GetKey(obj), cmp2._GetKey(obj)};
      }
      public override int CompareKeys(JToken[] a, JToken[] b)
      {
         if (a.Length < 2 || b.Length < 2) throw new BMException("Both key arrays should have minimal length 2. Lenght a/b is {0}/{1}.", a.Length, b.Length);
         int rc = cmp1.CompareKey(a[0], b[0]);
         return rc == 0 ? cmp2.CompareKey(a[1], b[1]) : rc;
      }

      public override int GetHash(JObject obj)
      {
         return cmp1.GetHash(obj) ^ cmp2.GetHash(obj);
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         int n2;
         int ret = cmp1.GetHash(obj, out nullIndex) ^ cmp2.GetHash(obj, out n2);
         if (n2 >= 0 && nullIndex < 0)
            nullIndex = 1;
         return ret;
      }
   }

   class ComparerN : JComparer
   {
      private readonly Comparer1Base[] cmps;
      public ComparerN(Comparer1Base[] cmps)
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


      public override JToken[] GetKeys(JObject obj)
      {
         var ret = new JToken[cmps.Length];
         for (int i = 0; i < cmps.Length; i++)
            ret[i] = cmps[i]._GetKey(obj);
         return ret;
      }
      public override int CompareKeys(JToken[] a, JToken[] b)
      {
         if (a.Length < cmps.Length || b.Length < cmps.Length) throw new BMException("Both key arrays should have minimal length {2}. Lenght a/b is {0}/{1}.", a.Length, b.Length, cmps.Length);
         for (int i = 0; i < cmps.Length; i++)
         {
            int rc = cmps[i].CompareKey(a[i], b[i]);
            if (rc != 0) return rc;
         }
         return 0;
      }


      public override int GetHash(JObject obj)
      {
         int ret = 0;
         for (int i = cmps.Length - 1; i >= 0; i--)
         {
            ret ^= cmps[i].GetHash(obj);
         }
         return ret;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         nullIndex = -1;
         int ret = 0;
         int ni;
         for (int i = cmps.Length - 1; i >= 0; i--)
         {
            ret ^= cmps[i].GetHash(obj, out ni);
            if (ni >= 0) nullIndex = i;
         }
         return ret;
      }
   }
}
