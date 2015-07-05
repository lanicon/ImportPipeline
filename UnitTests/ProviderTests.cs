using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.IO;
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

      private static String toString(IStreamProvider p)
      {
         FileStreamProvider fsp = p as FileStreamProvider;
         if (fsp == null) return p.ToString();

         StringBuilder sb = new StringBuilder();
         sb.Append(p.FullName);
         sb.Append(", r=");
         sb.Append(p.RelativeName);
         sb.Append(", v=");
         sb.Append(p.VirtualName);
         return sb.ToString();
      }
      private static void dumpProvider(TextWriter w, GenericStreamProvider p, PipelineContext ctx, String what)
      {
         w.WriteLine();
         w.WriteLine(what);
         w.WriteLine("Dumping roots");
         foreach (var r in p.GetRootElements(ctx))
            w.WriteLine("-- " + r);
         w.WriteLine("Dumping leafs");
         foreach (var r in p.GetElements(ctx))
            w.WriteLine("-- " + toString(r));
      }
   }
}
