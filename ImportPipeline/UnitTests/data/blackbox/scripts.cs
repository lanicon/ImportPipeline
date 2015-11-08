using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.ImportPipeline;

   public class ScriptExtensions
   {
      public Object OldScript (PipelineContext ctx, String key, Object value)
      {
         if (key != "record")
            throw new BMException ("OldScript should be called with key=record. Not with [{0}].", key);
         return value;
      }
      public Object NewScript (PipelineContext ctx, Object value)
      {
         if (value!=null && value.ToString() == "skip")
            ctx.ClearAllAndSetFlags (_ActionFlags.SkipAll, "record");
         return value;
      }

   }
