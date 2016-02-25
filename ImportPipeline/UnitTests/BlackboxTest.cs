/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.ImportPipeline.Template;
using System.Reflection;
using System.IO;
using System.Text;
using Bitmanager.Core;
using Bitmanager.ImportPipeline;
using Bitmanager.IO;

namespace UnitTests
{
   [TestClass]
   public class BlackboxTest : FileTestBase
   {
      [TestMethod]
      public void TestImports()
      {
         File.Delete(newDataRoot + "cmd_out.txt"); //this is needed because the add on the ds/_start has no value and gets ignored

         ImportEngine eng = new ImportEngine();
         eng.Load(root + "import.xml");
         var report = eng.Import("json,jsoncmd,tika_raw,tika_sort_title,tika_undup_title");
         Console.WriteLine(report);
         Assert.AreEqual(null, report.ErrorMessage);

         CheckFiles("cmd_out.txt");
         CheckFiles("json_out.txt");
         CheckFiles("tika_raw.txt");
         CheckFiles("tika_sort_title.txt");
         CheckFiles("tika_undup_title.txt");

         Assert.AreEqual(5, report.DatasourceReports.Count);
         int i = -1;
         checkDataSourceStats(report.DatasourceReports[++i], 5, 5);//The string value will not be added, bcause its emitted as 'record'. It is 5/5 because there are 2 EP's. Maybe we need to do something for the string value...
         checkDataSourceStats(report.DatasourceReports[++i], 5, 5);
         checkDataSourceStats(report.DatasourceReports[++i], 10, 10);
         checkDataSourceStats(report.DatasourceReports[++i], 10, 10);
         checkDataSourceStats(report.DatasourceReports[++i], 10, 3);
      }

      private void checkDataSourceStats (DatasourceReport rep, int expEmitted, int expAdded)
      {
         Assert.AreEqual(null, rep.ErrorMessage);
         Assert.AreEqual(expEmitted, rep.Emitted);
         Assert.AreEqual(expAdded, rep.Added);
      }
      [TestMethod]
      public void TestCommands()
      {
         File.Delete(newDataRoot + "cmd-out.txt");
         ImportEngine eng = new ImportEngine();
         eng.Load(root + "import.xml");
         var report = eng.Import("jsoncmd");
         Assert.AreEqual(1, report.DatasourceReports.Count);
         Assert.AreEqual(null, report.ErrorMessage);
         var dsReport = report.DatasourceReports[0];
         Console.WriteLine("Report: {0}", dsReport);
         Assert.AreEqual(5, dsReport.Emitted);
         Assert.AreEqual(4, dsReport.Added);

         CheckFiles("cmd-out.txt");
      }
   }
}
