using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.ImportPipeline;

namespace UnitTests
{
   [TestClass]
   public class SwitchesTest
   {
      [TestMethod]
      public void TestSwitches()
      {
         var sw = new Switches("test1, test2, -test1, -test3, -test4, test4 test6 test7");
         Assert.AreEqual(false, sw.IsOn("test5"));
         Assert.AreEqual(false, sw.IsOn("test1"));
         Assert.AreEqual(true, sw.IsOn("test2"));
         Assert.AreEqual(false, sw.IsOn("test3"));
         Assert.AreEqual(true, sw.IsOn("test4"));

         Assert.AreEqual(true, sw.IsOff("test1"));
         Assert.AreEqual(false, sw.IsOff("test2"));
         Assert.AreEqual(true, sw.IsOff("test3"));
         Assert.AreEqual(false, sw.IsOff("test4"));
         Assert.AreEqual(false, sw.IsOff("test5"));

         Assert.AreEqual("test6, test7", sw.GeUnknownSwitches());
         Assert.AreEqual("test1, test2, test3, test4, test5", sw.GeAskedSwitches());
      }
   }
}
