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
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public class MemoryBasedMapperWriters : MapperWritersBase, IEnumerable<JObject>
   {
      readonly JComparer comparer;
      readonly JComparer hasher;
      List<JObject>[] writers;

      public MemoryBasedMapperWriters(JComparer hasher, JComparer comparer, int cnt)
      {
         this.hasher = hasher;
         this.comparer = comparer;
         writers = new List<JObject>[cnt];
         if (cnt != 1 && hasher == null)
            throw new BMException("Cannot create MemoryBasedMapperWriters without a hasher if fanout={0}.", cnt);
         createLists();
      }

      private void createLists()
      {
         for (int i = 0; i < writers.Length; i++)
         {
            writers[i] = new List<JObject>();
         }
      }

      /// <summary>
      /// Writes the data to the appropriate file (designed by the hash)
      /// </summary>
      public override void Write(JObject data)
      {
         if (hasher==null)
            writers[0].Add(data);
         else
         {
            uint hash = (uint)hasher.GetHash(data);
            uint file = (hash % (uint)writers.Length);
            writers[file].Add(data);
         }
      }


      /// <summary>
      /// Optional writes the data to the appropriate file (designed by the hash)
      /// If the index of the first key that had a null value >= minNullIndex, the value is not written.
      /// 
      /// The returnvalue reflects whether has been written or not.
      /// </summary>
      public override bool OptWrite(JObject data, int maxNullIndex = -1)
      {
         int nullIndex;
         uint hash = (uint)hasher.GetHash(data, out nullIndex);
         if (nullIndex > maxNullIndex) return false;
         uint file = (hash % (uint)writers.Length);
         writers[file].Add (data);
         return true;
      }

      public override void Dispose()
      {
         createLists();
      }

      public override MappedObjectEnumerator GetObjectEnumerator(int index, bool buffered=false)
      {
         if (index < 0 || index >= writers.Length) return null;
         return new MemoryObjectEnumerator(writers[index], index);
      }


      public IEnumerator<JObject> GetEnumerator()
      {
         for (int index = 0; index < writers.Length; index++)
         {
            var e = GetObjectEnumerator(index);
            try
            {
               while (true)
               {
                  var obj = e.GetNext();
                  if (obj == null) break;
                  yield return obj;
               }
            }
            finally
            {
               Utils.FreeAndNil(ref e);
            }
         }
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }


      // --------------------------------------------------------------------------------------------- 
      // Enumerator objects
      // ---------------------------------------------------------------------------------------------
      public class MemoryObjectEnumerator : MappedObjectEnumerator
      {
         private readonly List<JObject> list;
         private int idx;
         public MemoryObjectEnumerator(List<JObject> list, int index)
            : base("<in_memory>", index)
         {
            this.list = list;
         }

         public override JObject GetNext()
         {
            if (idx >= list.Count) return null;
            return list[idx++];
         }
         public override List<JObject> GetAll()
         {
            if (idx == 0) return list; 
            var ret = new List<JObject>(4000);
            while (true)
            {
               JObject o = GetNext();
               if (o == null) break;
               ret.Add(o);
            }
            return ret;
         }

         public override void Close()
         {
         }

         public override void Dispose()
         {
         }

      }
   }

}
