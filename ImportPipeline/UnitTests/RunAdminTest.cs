using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.ImportPipeline;
using Bitmanager.IO;
using System.Reflection;

namespace UnitTests
{
   [TestClass]
   public class RunAdminTest
   {
      [TestMethod]
      public void TestRunAdmins()
      {
         var eng = new ImportEngine();

         var fn = IOUtils.FindFileToRoot(Assembly.GetExecutingAssembly().Location+"\\..", "runadmin.txt", FindToTootFlags.ReturnOriginal);
         Console.WriteLine("Loading from {0}", fn);
         var settings = new RunAdministrationSettings(eng, fn, 50, -1);
         var runs = new RunAdministrations(settings);
         runs.Load(fn);
         runs.Save(fn + ".new");
         Assert.AreEqual(17, runs.Count);

         settings = new RunAdministrationSettings(eng, null, 3, -1);
         runs = new RunAdministrations(settings);
         var run = addRun(runs, "a", -2);
         run = addRun(runs, "b", -2);
         run = addRun(runs, "a", -4);
         run = addRun(runs, "a", -6);
         Assert.AreEqual(4, runs.Count);
         Assert.AreEqual(2, runs.IndexOf(run));
         run = addRun(runs, "a", -8);
         Assert.AreEqual(4, runs.Count);
         Assert.AreEqual(-1, runs.IndexOf(run));
         run = addRun(runs, "a", -8, _ImportFlags.ImportFull);
         Assert.AreEqual(4, runs.Count);
         Assert.AreEqual(2, runs.IndexOf(run));
         run = addRun(runs, "a", -7, _ImportFlags.ImportFull);
         Assert.AreEqual(4, runs.Count);
         Assert.AreEqual(1, runs.IndexOf(run));
         run = addRun(runs, "a", -5);
         Assert.AreEqual(4, runs.Count);
         Assert.AreEqual(-1, runs.IndexOf(run));
         run = addRun(runs, "a", -1);
         Assert.AreEqual(4, runs.Count);
         Assert.AreEqual(0, runs.IndexOf(run));
         for (int i=2; i<10; i++)
         {
            run = addRun(runs, "a", -i);
            Assert.AreEqual(4, runs.Count);
            Assert.AreEqual(-1, runs.IndexOf(run));

         }

      }

      RunAdministration addRun (RunAdministrations list, String ds, int days, _ImportFlags flags = (_ImportFlags)0, int added=0)
      {
         var obj = new RunAdministration(ds, DateTime.Now.AddDays(days), flags, added);
         list.Add(obj);
         Console.WriteLine();
         foreach (var a in list)
            Console.WriteLine("-- " + a);
         return obj;
      }
   }
}
