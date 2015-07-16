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

      public static JHasher Create(List<KeyAndType> list)
      {
         if (list == null || list.Count == 0) return new JHasher();
         switch (list.Count)
         {
            case 1: return new Hasher1(list[0]);
            case 2: return new Hasher2(list[0], list[1]);
            default: return new HasherN(list.ToArray());
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

   class Hasher1 : JHasher
   {
      private readonly JPath path;
      public Hasher1(KeyAndType keyAndType)
      {
         path = keyAndType.Key;
      }
      public override int GetHash(JObject obj)
      {
         JToken tk = path.Evaluate(obj, JEvaluateFlags.NoExceptMissing);
         return tk == null ? NULLHASH : tk.GetHashCode();
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         JToken tk = normalizeToken(path.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
         if (tk==null)
         {
            nullIndex = 0;
            return NULLHASH;
         }
         nullIndex = -1;
         return tk.GetHashCode();
      }
   }

   class Hasher2 : JHasher
   {
      private readonly JPath path1, path2;
      public Hasher2(KeyAndType p1, KeyAndType p2)
      {
         path1 = p1.Key;
         path2 = p2.Key;
      }
      public override int GetHash(JObject obj)
      {
         JToken tk = normalizeToken(path1.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
         int ret = tk == null ? NULLHASH : tk.GetHashCode();
         tk = normalizeToken(path2.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
         return ret ^ (tk == null ? NULLHASH : tk.GetHashCode());
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         nullIndex = -1;
         JToken tk = normalizeToken(path1.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
         int ret;
         if (tk == null)
         {
            nullIndex = 0;
            ret = NULLHASH;
         }
         else
         {
            ret = tk.GetHashCode(); 
         }
         tk = normalizeToken(path2.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
         if (tk == null)
         {
            if (nullIndex < 0) nullIndex = 1;
         }
         else
         {
            ret ^= tk.GetHashCode();
         }
         return ret;
      }
   }
   class HasherN : JHasher
   {
      private readonly KeyAndType[] keyAndTypes;
      public HasherN(KeyAndType[] keyAndTypes)
      {
         this.keyAndTypes = keyAndTypes;
      }
      public override int GetHash(JObject obj)
      {
         int ret = 0;
         for (int i = keyAndTypes.Length - 1; i >= 0; i--)
         {
            JToken tk = normalizeToken(keyAndTypes[i].Key.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
            ret ^= (tk == null ? NULLHASH : tk.GetHashCode());
         }
         return ret;
      }
      public override int GetHash(JObject obj, out int nullIndex)
      {
         nullIndex = -1;
         int ret = 0;
         for (int i = keyAndTypes.Length - 1; i >= 0; i--)
         {
            JToken tk = normalizeToken(keyAndTypes[i].Key.Evaluate(obj, JEvaluateFlags.NoExceptMissing));
            if (tk==null)
            {
               nullIndex = i;
               ret ^= NULLHASH;
               continue;
            }
            ret ^= tk.GetHashCode();
         }
         return ret;
      }
   }
}
