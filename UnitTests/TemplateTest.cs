﻿using System;
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

      private static String resultAsString(TemplateEngine eng)
      {
         StringBuilder sb = new StringBuilder();
         var rdr = eng.ResultAsReader();
         while (true)
         {
            String line = rdr.ReadLine();
            if (line==null) break;
            if (sb.Length>0) sb.Append('|');
            sb.Append(line);
         }
         return sb.ToString();
      }
      [TestMethod]
      public void TestSimple()
      {
         TemplateEngine eng = new TemplateEngine(new TemplateSettings());

         eng.LoadFromFile(root + "templates\\simple.txt");
         Assert.AreEqual(" included regel met 'abc'| this line is in between| included regel met ''| ", resultAsString(eng));

      }

      [TestMethod]
      public void TestSimpleRecursive()
      {
         var settings = new TemplateSettings(true, 10);
         TemplateEngine eng = new TemplateEngine(settings);
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
         var settings = new TemplateSettings(true, 10);
         TemplateEngine eng = new TemplateEngine(settings);
         var v = eng.Variables;
         v.Set("boe", "bah");
         v.Set("var", "Dit is $$var$$");
         eng.LoadFromFile(root + "templates\\simple.txt");
         Assert.AreEqual(" included regel met 'abc'| this line is in between| included regel met 'Dit is bah'| ", resultAsString(eng));
      }


   }
}