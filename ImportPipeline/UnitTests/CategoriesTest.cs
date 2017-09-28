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
using Bitmanager.ImportPipeline;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;

namespace UnitTests
{
   [TestClass]
   public class CategoriesTest
   {
      [TestMethod]
      public void TestCategories()
      {
         using (ImportEngine eng = new ImportEngine())
         {
            XmlHelper xml = new XmlHelper();
            xml.LoadXml("<category/>");
            xml.WriteVal("@name", "boo");
            xml.WriteVal("@cat", "self");
            xml.WriteVal("@dstfield", "cat");
            var sel = xml.DocumentElement.AddElement("select");
            sel.SetAttribute("field", "name");
            sel.SetAttribute("expr", "weerd");

            Category cat = Category.Create(xml.DocumentElement);

            PipelineContext ctx = new PipelineContext(eng);
            EndpointWrapper ep = new EndpointWrapper(eng, xml.DocumentElement);
            IDataEndpoint dep = ep.CreateDataEndpoint(ctx, "abc");

            dep.SetField("name", "peter weerd");
            bool handled = cat.HandleRecord(ctx, dep, (JObject)dep.GetField(null));
            Assert.IsTrue(handled);
            Assert.AreEqual("self", dep.GetFieldAsStr("cat"));
            dep.SetField("name", "peter weerd peter");
            handled = cat.HandleRecord(ctx, dep, (JObject)dep.GetField(null));
            Assert.IsTrue(handled);
            Assert.AreEqual("self;self", dep.GetFieldAsStr("cat"));
         }
      }
   }

   public class EndpointWrapper : Endpoint
   {
      public EndpointWrapper (ImportEngine eng, XmlNode node) : base(eng, node)
      { }

      public IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string name)
      {
         return base.CreateDataEndpoint(ctx, name, true);
      }

   }
}
