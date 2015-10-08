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
using Bitmanager.IO;
using Bitmanager.Core;
using Bitmanager.ImportPipeline;
using Bitmanager.Xml;
using System.IO;
using System.Reflection;
using Bitmanager.ImportPipeline.StreamProviders;
using System.Text;
using System.Xml;

namespace UnitTests
{
   [TestClass]
   public class ProviderTests
   {
      String root = IOUtils.FindDirectoryToRoot(Assembly.GetExecutingAssembly().Location, "data\\providers", FindToTootFlags.Except) + "\\";
      [TestMethod]
      public void TestMethod1()
      {
         Console.WriteLine(root);
         using (ImportEngine eng = new ImportEngine())
         {
            eng.Load(root + "providers.xml");

            PipelineContext ctx = new PipelineContext(eng, eng.Datasources[0]);
            XmlHelper xml = new XmlHelper(root + "providers.xml");

            using (FileStream actual = new FileStream(root + "actual.txt", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4096))
            {
               using (TextWriter w = actual.CreateTextWriter())
               {
                  foreach (XmlNode node in xml.SelectSingleNode("tests").ChildNodes)
                  {
                     if (node.NodeType != XmlNodeType.Element) continue;
                     GenericStreamProvider p = new GenericStreamProvider();
                     p.Init(ctx, node);
                     dumpProvider(w, p, ctx, node.LocalName);
                  }

                  w.Flush();
                  byte[] exp = File.ReadAllBytes(root + "expected.txt");
                  actual.Position = 0;
                  byte[] act = actual.ReadAllBytes();
                  Assert.AreEqual(exp.Length, act.Length);
                  for (int i = 0; i < exp.Length; i++)
                     Assert.AreEqual(exp[i], act[i]);
               }
            }
         }
      }

      private String toString(IStreamProvider p)
      {
         FileStreamProvider fsp = p as FileStreamProvider;
         if (fsp == null) return p.ToString();

         StringBuilder sb = new StringBuilder();
         sb.Append(p.FullName);
         sb.Append(", r=");
         sb.Append(p.RelativeName);
         sb.Append(", v=");
         sb.Append(p.VirtualName);
         return replaceRoot(sb.ToString());
      }

      private String replaceRoot (String x)
      {
         if (String.IsNullOrEmpty(x)) return x;
         return x.ReplaceEx(IOUtils.DelSlash(root), @"<ROOT>", StringComparison.OrdinalIgnoreCase);
      }
      private void dumpProvider(TextWriter w, GenericStreamProvider p, PipelineContext ctx, String what)
      {
         w.WriteLine();
         w.WriteLine(what);
         w.WriteLine("Dumping roots");
         foreach (var r in p.GetRootElements(ctx))
            w.WriteLine("-- " + replaceRoot(r.ToString()));
         w.WriteLine("Dumping leafs");
         foreach (var r in p.GetElements(ctx))
            w.WriteLine("-- " + toString(r));
      }
   }
}
