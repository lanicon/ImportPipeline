using Bitmanager.Core;
using Bitmanager.ImportPipeline.StreamProviders;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public class InputStreamWithHeader : StreamWrapperBase
   {
      public readonly byte[] Header;
      private long pos;

      public InputStreamWithHeader(Stream wrapped): base (wrapped)
      {
         if (wrapped.CanSeek)
         {
            if (wrapped.Position != 0)
               throw new BMException("InputStreamWithHeader only supports streams that are at position 0, but current pos={0}.", wrapped.Position);
         }
         byte[] b = new byte[256];
         int num = wrapped.Read(b, 0, b.Length);
         if (num == b.Length)
            Header = b;
         else
         {
            Header = new byte[num];
            if (num > 0)
               Array.Copy (b, Header, num);
         }
         pos = 0;
      }

      public override bool CanWrite
      {
         get
         {
            return false;
         }
      }

      public override long Position
      {
         get
         {
            return pos;
         }
         set
         {
            if (value < 0) value = 0;
            pos = value;
            baseStream.Position = value < Header.Length ? Header.Length : value;
         }
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         if (count<=0) return 0;
         int cnt = (int)(Header.Length - pos);
         int fromStream = 0;
         if (cnt > 0)
         {
            if (count < cnt) cnt = count;
            Array.Copy(Header, (int)pos, buffer, offset, cnt);
            offset += cnt;
            count -= count;
            if (count > 0)
            {
               fromStream = baseStream.Read(buffer, offset, count);
               cnt += fromStream;
            }
         }
         else
         {
            cnt = baseStream.Read(buffer, offset, count);
         }

         pos += cnt;
         return cnt;
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         long newPos = offset;
         switch (origin)
         {
            case SeekOrigin.Begin:
               newPos = offset;
               break;
            case SeekOrigin.Current: 
               newPos = pos + offset; break;
            case SeekOrigin.End: 
               newPos = baseStream.Seek(offset, origin);
               pos = newPos;
               if (newPos < Header.Length)
                  baseStream.Seek (Header.Length, SeekOrigin.Begin);
               return pos;
         }
         baseStream.Seek(newPos >= Header.Length ? newPos : Header.Length, SeekOrigin.Begin);
         pos = newPos;
         return newPos;
      }

      private static void throwUnsupported()
      {
         throw new NotSupportedException ("Not supported by InputStreamWithHeader");
      }

      public override void SetLength(long value)
      {
         throwUnsupported();
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         throwUnsupported();
      }

      public enum HeaderType {Unknown, GZ, ZIP, SevenZip};
      public HeaderType GetHeaderType (out int headerLen)
      {
         if (Header.Length > 3 && Header[0] == (byte)0x1F && Header[1] == (byte)0x8B && Header[2] == (byte)0x08)
         {
            headerLen = 3;
            return HeaderType.GZ;
         }

         headerLen = 0;
         return HeaderType.Unknown;
      }

   }
}
