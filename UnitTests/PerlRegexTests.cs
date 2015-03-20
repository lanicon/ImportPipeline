using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.RegularExpressions;

namespace UnitTests
{
   [TestClass]
   public class PerlRegexTests
   {
      [TestMethod]
      public void TestMethod1()
      {
         Regex expr = new Regex("^(.*)"); //zonder ^ is het resultaat xabcx (ivm lege match)
         String x = expr.Replace("abc", "x$1");
         Assert.AreEqual("xabc", x);
      }
   }
}
