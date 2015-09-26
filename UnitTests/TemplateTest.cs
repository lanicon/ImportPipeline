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

namespace UnitTests
{
   [TestClass]
   public class TemplateTest
   {
      String root;
      public TemplateTest()
      {
         root = Path.GetFullPath(Assembly.GetExecutingAssembly().Location + @"\..\..\..\");
      }

      [TestMethod]
      public void TestSimple()
      {
         ITemplateEngine eng = new TemplateFactory().CreateEngine();

         eng.LoadFromFile(root + "templates\\simple.txt");
         Assert.AreEqual(" included regel met 'abc'| this line is in between| included regel met ''| ", resultAsString(eng));

      }

      [TestMethod]
      public void TestSimpleRecursive()
      {
         var factory = new TemplateFactory();
         factory.AutoWriteGenerated = true;
         factory.DebugLevel = 10;

         ITemplateEngine eng = factory.CreateEngine();
         var v = eng.Variables;
         v.Set("boe", "bah");
         v.Set("var", "Dit is $$boe$$");
         eng.LoadFromFile(root + "templates\\simple.txt");
         Assert.AreEqual(" included regel met 'abc'| this line is in between| included regel met 'Dit is bah'| ", resultAsString(eng));
      }

      [TestMethod]
      [ExpectedException(typeof(BMException))]
      public void TestVarRecursionToDeep()
      {
         var factory = new TemplateFactory();
         factory.AutoWriteGenerated = true;
         factory.DebugLevel = 10;
         ITemplateEngine eng = factory.CreateEngine();
         var v = eng.Variables;
         v.Set("boe", "bah");
         v.Set("var", "Dit is $$var$$");
         eng.LoadFromFile(root + "templates\\simple.txt");
         Assert.AreEqual(" included regel met 'abc'| this line is in between| included regel met 'Dit is bah'| ", resultAsString(eng));
      }

      private static String resultAsString(ITemplateEngine eng)
      {
         StringBuilder sb = new StringBuilder();
         var rdr = eng.ResultAsReader();
         while (true)
         {
            String line = rdr.ReadLine();
            if (line == null) break;
            if (sb.Length > 0) sb.Append('|');
            sb.Append(line);
         }
         return sb.ToString();
      }

   }
}
