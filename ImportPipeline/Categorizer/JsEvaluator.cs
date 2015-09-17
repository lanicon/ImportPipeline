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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.CodeDom.Compiler;
using System.Reflection;
using Microsoft.JScript;
using Microsoft.JScript.Vsa;

namespace Bitmanager.ImportPipeline
{
   public class JsEvaluator
   {
      public static object EvalToObject(string statement)
      {
        return _evaluatorType.InvokeMember("Eval",BindingFlags.InvokeMethod,null,_evaluator,new object[] { statement });
      }
      static JsEvaluator()
      {
         //VsaEngine eng = new VsaEngine(true);
         //Closure
         //en
         CodeDomProvider c = new JScriptCodeProvider();
         CompilerParameters parameters = new CompilerParameters(); ;
         parameters.GenerateInMemory = true;
         CompilerResults results = c.CompileAssemblyFromSource(parameters, _jscriptSource);
         Assembly assembly = results.CompiledAssembly;
         _evaluatorType = assembly.GetType("Evaluator.Evaluator");
         _evaluator = Activator.CreateInstance(_evaluatorType);
      }
      private static object _evaluator;
      private static Type _evaluatorType;
      private static readonly string _jscriptSource =  
         @"package Evaluator
         {
            class Evaluator
            {
               public function Eval(expr : String) : Object
               {
                  return eval(expr);
               }
            }
         }";
   }
}
