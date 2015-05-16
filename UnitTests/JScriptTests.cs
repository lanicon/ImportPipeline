using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.JScript.Vsa;
using Bitmanager.Core;
using Microsoft.JScript;
using Newtonsoft.Json.Linq;

namespace UnitTests
{
   [TestClass]
   public class JScriptTests
   {
      VsaEngine eng = VsaEngine.CreateEngine();

      public object EvalJScript(string src)
      {
         return Eval.JScriptEvaluate(src, eng);
      }

      [TestMethod]
      public void TestMethod1()
      {
         //JScriptException
         //dump ("function x(aa,bb) {return aa + bb;}");
         Closure c = (Closure)dump("(function (aa,bb) {return aa + bb;})");
         dump(c, 1, (long)3);

         c = (Closure)dump("(function (aa) {return aa[\"x\"]<'acc';})");
         var obj = new JObject();
         obj["x"] = "abc";
         Console.WriteLine("args=" + c.arguments);
         Console.WriteLine("args=" + (c.arguments==null? null : c.arguments.GetType().FullName));
         dump(c, obj);
         Console.WriteLine("args=" + c.arguments);
         Console.WriteLine("args=" + (c.arguments == null ? null : c.arguments.GetType().FullName));

      }

      private Object dump (Closure c, params Object[] args)
      {
         Object ret = c.Invoke(null, args);
         Console.WriteLine("invoke=" + c);
         Console.WriteLine("ret={0}: {1}", ret==null ? "null" : ret.GetType().Name, ret);
         return ret;
      }
      private Object dump (String src)
      {
         Object ret = EvalJScript(src);
         Console.WriteLine();
         Console.WriteLine("js=" + src);
         Console.WriteLine("ret={0}: {1}", ret == null ? "null" : ret.GetType().Name, ret);
         return ret;
      }
   }

   abstract class A
   {
      public A()
      {
         m();
         k();
      }

      protected virtual void m()
      {
         Console.WriteLine("A:m");
      }
      protected abstract void k();
   }
   class B:A
   {
      protected override void m()
      {
         Console.WriteLine("B:m");
      }
      protected override void k()
      {
         Console.WriteLine("B:k");
      }

   }
}
