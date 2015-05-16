using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   /// <summary>
   /// Wraps a Stream by passing al methods through the base stream
   /// </summary>
   public class StreamWrapperBase : Stream
   {
      protected Stream baseStream;

      protected override void Dispose(bool disposing)
      {
         Utils.FreeAndNil(ref baseStream);
      }

      public StreamWrapperBase(Stream stream)
      {
         baseStream = stream;
      }

      public override bool CanRead
      {
         get
         {
            return baseStream.CanRead;
         }
      }

      public override bool CanSeek
      {
         get
         {
            return baseStream.CanSeek;
         }
      }

      public override bool CanWrite
      {
         get
         {
            return baseStream.CanWrite;
         }
      }

      public override void Flush()
      {
         baseStream.Flush();
      }

      public override long Length
      {
         get
         {
            return baseStream.Length;
         }
      }

      public override long Position
      {
         get
         {
            return baseStream.Position;
         }
         set
         {
            baseStream.Position = value;
         }
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         return baseStream.Read(buffer, offset, count);
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         return baseStream.Seek(offset, origin);
      }

      public override void SetLength(long value)
      {
         baseStream.SetLength(value);
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         baseStream.Write(buffer, offset, count);
      }
   }
}