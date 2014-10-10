using Bitmanager.Core;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Xml;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   [Flags]
   public enum _ActionFlags
   {
      None = 0,
      Skip = 1 << 0,
      SkipRest = 1 << 2,
      SkipAll = Skip | SkipRest,
      Handled = 1 << 8,
      Skipped = 1 << 9,
   };
   [Flags]
   public enum _ErrorState
   {
      OK = 0,
      Error = 1 << 0,
      Limited = 1 << 2,
   };

   public class PipelineContext
   {
      public readonly ImportEngine ImportEngine;
      public readonly Pipeline Pipeline;
      public readonly DatasourceAdmin DatasourceAdmin;
      public readonly Logger ImportLog;
      public readonly Logger DebugLog;
      public readonly Logger ErrorLog;
      public readonly Logger MissedLog;
      public MaxAddsExceededException Exceeded { get; private set; }
      public PipelineAction Action;
      public String SkipUntilKey;
      public int Added, Deleted, Skipped, Emitted, Errors;
      public int LogAdds;
      public int MaxAdds;
      public _ImportFlags ImportFlags;
      public _ActionFlags ActionFlags;
      public _ErrorState ErrorState;

      public PipelineContext(ImportEngine eng, DatasourceAdmin ds)
      {
         ImportEngine = eng;
         DatasourceAdmin = ds;
         Pipeline = ds.Pipeline;
         ImportLog = eng.ImportLog.Clone (ds.Name);
         DebugLog = eng.DebugLog.Clone(ds.Name);
         ErrorLog = eng.ErrorLog.Clone(ds.Name);
         MissedLog = eng.MissedLog.Clone(ds.Name);
         ImportFlags = eng.ImportFlags;
         LogAdds = (ds.LogAdds > 0) ? ds.LogAdds : eng.LogAdds;
         MaxAdds = (ds.MaxAdds >= 0) ? ds.MaxAdds : eng.MaxAdds;
      }
      public PipelineContext(ImportEngine eng)
      {
         ImportEngine = eng;
         ImportLog = eng.ImportLog;
         DebugLog = eng.DebugLog;
         ErrorLog = eng.ErrorLog;
         MissedLog = eng.MissedLog;
         ImportFlags = eng.ImportFlags;
      }

      internal PipelineAction SetAction(PipelineAction act)
      {
         ActionFlags |= _ActionFlags.Handled;
         Action = act;
         return act;
      }
      public Object ClearAllAndSetFlags()
      {
         ActionFlags |= _ActionFlags.SkipAll;
         Pipeline.ClearVariables();
         Action.Endpoint.Clear();
         return null;
      }
      public Object ClearAllAndSetFlags(_ActionFlags fl)
      {
         ActionFlags |= fl;
         Pipeline.ClearVariables();
         Action.Endpoint.Clear();
         return null;
      }
      public Object ClearAllAndSetFlags(_ActionFlags fl, String skipUntilKey)
      {
         ActionFlags |= fl;
         Action.Endpoint.Clear();
         Pipeline.ClearVariables();
         SkipUntilKey = skipUntilKey;
         return null;
      }

      public void CountAndLogAdd()
      {
         switch ((++Added) % LogAdds)
         {
            case 0: ImportLog.Log(_LogType.ltTimer, "Added {0} records", Added); break;
            case 1: if (Added != 1) break; ImportLog.Log(_LogType.ltTimerStart, "Added 1 record"); break;
         }
         if (MaxAdds >= 0 && Added > MaxAdds)
            throw Exceeded = new MaxAddsExceededException(Added);
      }
      public void LogLastAdd()
      {
         if (Added == 0)
            ImportLog.Log("No records were added.");
         else
            ImportLog.Log(_LogType.ltTimerStop, "Added {0} records", Added); 
      }

      public IDatasourceFeeder CreateFeeder(XmlNode node, String expr, Type defaultFeederType=null)
      {
         String feederType = defaultFeederType == null ? node.ReadStr(expr) : node.OptReadStr (expr, defaultFeederType.FullName);
         IDatasourceFeeder feeder = ImportEngine.CreateObject<IDatasourceFeeder>(feederType);
         feeder.Init(this, node);
         return feeder;
      }

      public IDatasourceFeeder CreateFeeder(XmlNode node, Type defaultFeederType = null)
      {
         String type = node.OptReadStr("@provider", null);
         if (type == null)
         {
            XmlNode child = node.SelectSingleNode("provider");
            if (child != null) return CreateFeeder(child, "@type");
         }
         return CreateFeeder(node, "@provider", defaultFeederType);
      }

      public String GetStats()
      {
         return String.Format("Emitted={3}, Added={0}, Deleted={1}, Skipped={2}, Errors={4}", Added, Deleted, Skipped, Emitted, Errors);
      }
   }

   public class MaxAddsExceededException : Exception
   {
      public MaxAddsExceededException(int limit) : base(String.Format("Max #adds exceeded: {0}.", limit)) { }

      public static bool ContainsMaxAddsExceededException(Exception e)
      {
         for (; e != null; e = e.InnerException)
         {
            if (e is MaxAddsExceededException) return true;
         }
         return false;
      }
   }
}
