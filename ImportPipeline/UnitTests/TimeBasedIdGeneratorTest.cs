using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.ImportPipeline;
using System.Threading;

namespace UnitTests
{
   [TestClass]
   public class TimeBasedIdGeneratorTest
   {
      [TestMethod]
      public void TestTimeBasedIdGenerator()
      {
         var gen = new TimeBasedIdGenerator();

         long id0 = gen.GetNextId();
         Thread.Sleep(1000);
         long id1 = gen.GetNextId();

         Assert.AreEqual(1, (id1 & 0xFFFFFF) - (id0 & 0xFFFFFF));

         long diff = (id1>>24) - (id0>>24);
         Console.WriteLine("IDs={0:X} , {1:X}, XOR={2:X}", id0, id1, 0);
         Console.WriteLine("diff=" + diff);
         Assert.AreEqual(1000.0, diff, 10.0);

         String s1 = gen.GetNextIdAsString();
         String s2 = gen.GetNextIdAsString();
         Console.WriteLine("S1=" + s1);
         Console.WriteLine("S2=" + s2);
         Assert.AreNotEqual(s1, s2);
      }
   }
}
