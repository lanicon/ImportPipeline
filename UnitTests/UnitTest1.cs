using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;
using Bitmanager.ImportPipeline;

namespace UnitTests
{
   [TestClass]
   public class UnitTest1
   {
      [TestMethod]
      public void TestMethod1()
      {
         MemoryStream strm = new MemoryStream();
         StreamWriter wtr = new StreamWriter(strm, Encoding.UTF8);
         wtr.Write("F1\t  \" boe en bah \"   \t123 \t456\r\nrec2\r\n\"rec3 met\r\n \"\"crlf\"\"\"\r\n\r\n");
         wtr.Flush();
         strm.Position = 0;

         CsvReader csv = new CsvReader(strm);
         csv.SkipEmptyRecords = false;
         Assert.AreEqual(true, csv.NextRecord());
         Assert.AreEqual(0, csv.Line);
         Assert.AreEqual(4, csv.Fields.Count);
         Assert.AreEqual("F1", csv.Fields[0]);
         Assert.AreEqual(" boe en bah ", csv.Fields[1]);
         Assert.AreEqual("123 ", csv.Fields[2]);
         Assert.AreEqual("456", csv.Fields[3]);

         Assert.AreEqual(true, csv.NextRecord());
         Assert.AreEqual(1, csv.Line);
         Assert.AreEqual(1, csv.Fields.Count);
         Assert.AreEqual("rec2", csv.Fields[0]);

         Assert.AreEqual(true, csv.NextRecord());
         Assert.AreEqual(2, csv.Line);
         Assert.AreEqual(1, csv.Fields.Count);
         Assert.AreEqual("rec3 met\r\n \"crlf\"", csv.Fields[0]);

         Assert.AreEqual(true, csv.NextRecord());
         Assert.AreEqual(3, csv.Line);
         Assert.AreEqual(0, csv.Fields.Count);

         Assert.AreEqual(false, csv.NextRecord());
         Assert.AreEqual(false, csv.NextRecord());
         Assert.AreEqual(false, csv.NextRecord());
      }
   }
}
