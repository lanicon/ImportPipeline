using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public class TimeBasedIdGenerator
   {
      static readonly long ticks2000 = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc).Ticks;

      readonly Object _lock;
      long last_ticks;
      uint seq;


      public TimeBasedIdGenerator()
      {
         _lock = new Object();
      }

      public long GetNextId()
      {
         long ticks = Math.Max(last_ticks, (DateTime.UtcNow.Ticks - ticks2000) / 10000);
         lock (_lock)
         {
            seq = ((1+seq) &0xFFFFFF);
            if (seq==0) ticks++;

            last_ticks = ticks;
         }

         return ((ticks << 24)) | seq;
      }

      /// <summary>
      /// Return the ID in Big endian format. This makes it possible that the String-sort on ID equals the insertion order
      /// </summary>
      public byte[] GetNextIdAsBytes()
      {
         long id = GetNextId();
         byte[] ret = new byte[8];
         int shift = 64 - 8;
         for (int i = 0; i < ret.Length; i++, shift-=8)
            ret[i] = (byte)(id >> shift);
         return ret;
      }

      /// <summary>
      /// Return the ID in Big endian Base64 format. This makes it possible that the String-sort on ID equals the insertion order
      /// </summary>
      public String GetNextIdAsString()
      {
         byte[] b = GetNextIdAsBytes();
         return Base64Codec.UrlFriendly.Encode (b, 0, b.Length);
      }
   }
}
