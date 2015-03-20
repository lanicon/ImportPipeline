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
