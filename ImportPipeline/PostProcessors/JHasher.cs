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
   /// Hasher object. Computes the hash of an JObject by combining the one or more keys that are extracted from the object
   /// </summary>
   public class JHasher
   {
      protected static readonly int NULLHASH = 0; //x12345678; By using NULLHASH=0, all version of the hashers (1, 2, N) are more consistent...
      public virtual int GetHash(JObject obj)
      {
         return 0;
      }
      public virtual int GetHash(JObject obj, out int nullIndex)
      {
         nullIndex = -1;
         return 0;
      }

      //public static JHasher Create(List<KeyAndType> list)
      //{
      //   if (list == null || list.Count == 0) return new JHasher();
      //   switch (list.Count)
      //   {
      //      case 1: return new Hasher1(list[0]);
      //      case 2: return new Hasher2(list[0], list[1]);
      //      default: return new HasherN(list.ToArray());
      //   }
      //}
      public static JHasher Create(List<KeyAndType> keyAndTypes)
      {
         if (keyAndTypes == null || keyAndTypes.Count == 0) return new JHasher();
         switch (keyAndTypes.Count)
         {
            case 1: return Create(keyAndTypes[0]);
            case 2: return new Hasher2(Create(keyAndTypes[0]), Create(keyAndTypes[1]));
            default:
               var hashers = new Hasher1Base[keyAndTypes.Count];
               for (int i = 0; i < keyAndTypes.Count; i++)
                  hashers[i] = Create(keyAndTypes[i]);
               return new HasherN(hashers);
         }
      }

      private static Hasher1Base Create(KeyAndType keyAndType)
      {
         switch (keyAndType.Type & (CompareType.String | CompareType.Int | CompareType.Long | CompareType.Double | CompareType.Date))
         {
            case CompareType.String: return new Hasher1Str(keyAndType);
            case CompareType.Int: return new Hasher1Int(keyAndType);
            case CompareType.Long: return new Hasher1Long(keyAndType);
            case CompareType.Double: return new Hasher1Dbl(keyAndType);
            case CompareType.Date: return new Hasher1Date(keyAndType);
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
   abstract class Hasher1Base : JHasher
   {
      protected readonly JPath path;
      public Hasher1Base(KeyAndType keyAndType)
      {
         this.path = keyAndType.Key;
      }
   }

   class Hasher1Str : Hasher1Base
   {
      private readonly StringComparer stringComparer;
      private readonly StringComparison comparison;
      public Hasher1Str(KeyAndType keyAndType)
         : base(keyAndType)
      {
         this.comparison = (keyAndType.Type & CompareType.CaseInsensitive) != 0 ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
         this.stringComparer = (keyAndType.Type & CompareType.CaseInsensitive) != 0 ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
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
         return stringComparer.GetHashCode ((String)tk);

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

   class Hasher1Int : Hasher1Base
   {
      static readonly Comparer<int> cmp = Comparer<int>.Default;
      public Hasher1Int(KeyAndType keyAndType)
         : base(keyAndType)
      {
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


   class Hasher1Long : Hasher1Base
   {
      static readonly Comparer<long> cmp = Comparer<long>.Default;
      public Hasher1Long(KeyAndType keyAndType)
         : base(keyAndType)
      {
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
         return (int)tmp ^ (int)(tmp>>32);

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


   class Hasher1Dbl : Hasher1Base
   {
      static readonly Comparer<double> cmp = Comparer<double>.Default;
      public Hasher1Dbl(KeyAndType keyAndType)
         : base(keyAndType)
      {
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


   class Hasher1Date : Hasher1Base
   {
      static readonly Comparer<DateTime> cmp = Comparer<DateTime>.Default;
      public Hasher1Date(KeyAndType keyAndType)
         : base(keyAndType)
      {
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














   class Hasher2 : JHasher
   {
      private readonly Hasher1Base h1, h2;
      public Hasher2(Hasher1Base h1, Hasher1Base h2)
      {
         this.h1 = h1;
         this.h2 = h2;
      }
      public override int GetHash(JObject obj)
      {
         return h1.GetHash(obj) ^ h2.GetHash(obj);
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         int n2;
         int ret = h1.GetHash(obj, out nullIndex) ^ h2.GetHash(obj, out n2);
         if (n2 >= 0 && nullIndex < 0) 
             nullIndex = 1;
         return ret;
      }
   }
   class HasherN : JHasher
   {
      private readonly Hasher1Base[] hashers;
      public HasherN(Hasher1Base[] hashers)
      {
         this.hashers = hashers;
      }
      public override int GetHash(JObject obj)
      {
         int ret = 0;
         for (int i = hashers.Length - 1; i >= 0; i--)
         {
            ret ^= hashers[i].GetHash(obj);
         }
         return ret;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         nullIndex = -1;
         int ret = 0;
         int ni;
         for (int i = hashers.Length - 1; i >= 0; i--)
         {
            ret ^= hashers[i].GetHash(obj, out ni);
            if (ni >= 0) nullIndex = i;
         }
         return ret;
      }
   }
}
