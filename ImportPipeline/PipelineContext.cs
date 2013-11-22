using Bitmanager.Core;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class PipelineContext
   {
      public readonly ImportEngine ImportEngine;
      Logger addLogger = Logs.CreateLogger("pipelineAdder", "pipelineAdder");
      JObject accumulator;
      public PipelineContext(ImportEngine eng)
      {
         ImportEngine = eng;
         Clear();
      }

      public void Clear()
      {
         accumulator = new JObject();
      }

      public void SetField(String fld, Object value)
      {
         addLogger.Log("-- setfield {0}: '{1}'", fld, value);
         accumulator.WriteToken(fld, value);
      }

      public void Add(String[] toWhat)
      {
         addLogger.Log(accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
      }

      public static Object CreateObject(String objId)
      {
         if (objId != null)
         {
            switch (objId.ToLowerInvariant())
            {
               case "csv": return new CsvDatasource();
               case "urlprovider": return new UrlFeeder();
            }
         }
         return Objects.CreateObject (objId);
      }
   }
}
