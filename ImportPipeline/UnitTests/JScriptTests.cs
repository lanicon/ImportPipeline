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
      public void TestJScript()
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
