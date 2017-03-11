using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using Bitmanager.Core;
using Bitmanager.ImportPipeline.StreamProviders;
using System.Threading;

namespace UnitTests
{
   [TestClass]
   public class ProcessPipeStreamTest
   {
      [TestMethod]
      public void TestMethod1()
      {
         const int ALL=10000000;
         var strm = new ShellStreamProvider.ProcessPipeStream();
         var rdr = new Reader(strm);
         Action x = rdr.ReadAll;
         var asyncResult = x.BeginInvoke(null, null);

         Random rand = new Random (319);
         for (int i = 0; i < ALL; )
         {
            var N = 1+rand.Next (400);
            var buf = new byte[N];
            int j;
            for (j = 0; j < N && i < ALL; j++, i++)
            {
               buf[j] = (byte)i;
            }
            strm.Write(buf, 0, j);
            //Console.WriteLine("Write {0} bytes", j);
         }
         strm.Write(null, 0, 0);

         Console.WriteLine("Waiting");
         x.EndInvoke (asyncResult);
         Console.WriteLine("Terminated");

         Assert.AreEqual(ALL, rdr.offset);
      }
   }

   class Reader
   {
      public int offset;
      public readonly Stream strm;

      public Reader(ShellStreamProvider.ProcessPipeStream strm1)
      {
         this.strm = strm1;
      }

      int xx;
      public void ReadAll ()
      {
         var buf = new byte[256];
         while (true)
         {
            int cnt = strm.Read(buf, 0, buf.Length);
            //Console.WriteLine("Read returned {0} bytes", cnt);

            for (int i=0; i<cnt; i++, offset++)
            {
               if (buf[i] != (byte)offset)
                  throw new BMException ("Unexpected byte 0X{0:X} at offset 0x{1:X}", buf[i], offset);
            }
            if (cnt == 172)
               xx = cnt;
            if (cnt == 0) break;
         }
         Console.WriteLine("STOP READ");
      }
   }
}
