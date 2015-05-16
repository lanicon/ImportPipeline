using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.ImportPipeline.Template;
using Bitmanager.Core;

namespace UnitTests
{
   [TestClass]
   public class VariableTests
   {
      [TestMethod]
      public void TestMethod1()
      {
         IVariables v = new Variables();
         v.CopyFromDictionary(Environment.GetEnvironmentVariables());
         v.Dump(Logs.DebugLog, "geen reden");
      }
   }
}
