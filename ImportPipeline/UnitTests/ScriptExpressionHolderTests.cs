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
   public class ScriptExpressionHolderTest : FileTestBase
   {
      [TestMethod]
      public void TestSimpleExpression()
      {
         String fn = "testSimple.txt"; 
         ScriptExpressionHolder h = new ScriptExpressionHolder(null);
         h.AddExpression("f1", "12");
         h.AddExpression("f2", "value.ToString();");
         h.AddExpression("f3", "return value.ToString();");
         h.AddExpression("f4", "return value.ToString()");

         h.AddCondition("f5", "return value.ToString()");
         h.AddCondition("f6", "ctx.Action != null");

         Assert.AreEqual(6, h.Count);
         h.SaveAndClose(newDataRoot + fn);

         CheckFiles(fn);
      }

   }
}
