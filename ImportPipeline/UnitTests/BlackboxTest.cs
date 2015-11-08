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
      public void TestSimple()
      {
         ImportEngine eng = new ImportEngine();
         eng.Load(root + "import.xml");
         var report = eng.Import("json");
         Assert.AreEqual(1, report.DatasourceReports.Count);
         Assert.AreEqual(null, report.ErrorMessage);
         var dsReport = report.DatasourceReports[0];
         Console.WriteLine("Report: {0}", dsReport);
         Assert.AreEqual(5, dsReport.Emitted);
         Assert.AreEqual(4, dsReport.Added);

         CheckFiles("json-out.txt");
      }
   }
}
