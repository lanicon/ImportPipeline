using Bitmanager.Core;
//using Bitmanager.Elastic;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using Bitmanager.ImportPipeline;

   public class MyScript
   {
      public MyScript (PipelineContext ctx)
      {
         ctx.ImportLog.Log ("ctr Greetings from script");
      }
      public Object Test (PipelineContext ctx, String key, Object value)
      {
         ctx.ImportLog.Log ("Greetings from script");
         return value;
      }
   }
   