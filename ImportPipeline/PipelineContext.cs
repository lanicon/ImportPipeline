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
      ConditionMatched = 1<<10,
   };
   [Flags]
   public enum _ErrorState
   {
      OK = 0,
      Error = 1 << 0,
      Limited = 1 << 2,
      All = Error | Limited,
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
      public IPostProcessor PostProcessor;
      public IAdminEndpoint AdminEndpoint;
      public List<RunAdministration> RunAdministrations;
      public MaxAddsExceededException Exceeded { get; private set; }
      public Exception LastError { get; internal set; }
      public PipelineAction Action;
      public String SkipUntilKey;

      public int Added, Deleted, Skipped, Emitted, Errors, PostProcessed;
      public int LogAdds;
      public int MaxAdds;
      public int MaxEmits;
      public _ImportFlags ImportFlags;
      public _ActionFlags ActionFlags;
      public _ErrorState ErrorState;

      private bool itemStartPending;
      private Object valueForItemStart;

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
         MaxEmits = (ds.MaxEmits >= 0) ? ds.MaxEmits : eng.MaxEmits;
         if (MaxEmits < 0 && (ImportFlags & _ImportFlags.MaxAddsToMaxEmits) != 0)
            MaxEmits = MaxAdds;
         ImportLog.Log("Current maxAdds={0}, maxEmits={1}", MaxAdds, MaxEmits);
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
         //PW handled zou ook globaal uitgezet moeten kunnen worden
         if (!(act is PipelineNopAction))
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

      public bool HandleException(Exception err, String prefix="record", bool exceptIfNotHandled=true)
      {
         MaxAddsExceededException mae = err as MaxAddsExceededException;
         if (mae != null) throw new MaxAddsExceededException (mae);

         Errors++;
         String pfx = String.IsNullOrEmpty(prefix) ? "_error" : prefix + "/_error";
         Pipeline.HandleValue (this, pfx, err);
         if ((ActionFlags & _ActionFlags.Handled) != 0) return true;

         if (exceptIfNotHandled) throw new BMException (err, err.Message);
         return false;
      }


      public void IncrementEmitted ()
      {
         if (MaxEmits >= 0 && Emitted >= MaxEmits)
         {
            ImportLog.Log("MAX EMITS EXCEEDED, {0}", GetStats());
            throw Exceeded = new MaxAddsExceededException(Emitted, "emits");
         }
         switch ((++Emitted) % LogAdds)
         {
            case 0: 
               if ((this.ImportFlags & _ImportFlags.LogEmits) == 0)
                  if (Added != 0) break;
               ImportFlags |= _ImportFlags.LogEmits;
               ImportLog.Log(_LogType.ltTimer, "Emitted {0} records", Emitted); break;
         }
      }

      public void IncrementAndLogAdd()
      {
         if (MaxAdds >= 0 && Added >= MaxAdds)
         {
            ImportLog.Log("MAX ADDS EXCEEDED, {0}", GetStats());
            throw Exceeded = new MaxAddsExceededException(Added, "adds");
         }
         switch ((++Added) % LogAdds)
         {
            case 0: ImportLog.Log(_LogType.ltTimer, "Added {0} records", Added); break;
            case 1: if (Added != 1) break; ImportLog.Log(_LogType.ltTimerStart, "Added 1 record"); break;
         }
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
         String feederType = defaultFeederType == null ? node.ReadStr(expr) : node.ReadStr (expr, defaultFeederType.FullName);
         IDatasourceFeeder feeder = ImportEngine.CreateObject<IDatasourceFeeder>(feederType);
         feeder.Init(this, node);
         return feeder;
      }

      public IDatasourceFeeder CreateFeeder(XmlNode node, Type defaultFeederType = null)
      {
         String type = node.ReadStr("@provider", null);
         if (type == null)
         {
            XmlNode child = node.SelectSingleNode("provider");
            if (child != null) return CreateFeeder(child, "@type");
         }
         return CreateFeeder(node, "@provider", defaultFeederType);
      }

      public String GetStats()
      {
         return String.Format("Emitted={3}, Added={0}, PostProcessed={5}, Errors={4}, Deleted={1}, Skipped={2}", Added, Deleted, Skipped, Emitted, Errors, PostProcessed);
      }

      public Object SendItemStart(Object value = null)
      {
         valueForItemStart = value;
         itemStartPending = true;
         return Pipeline.HandleValue(this, "_item/_start", value);
      }
      public Object SendItemStop(Object value)
      {
         valueForItemStart = null;
         itemStartPending = false;
         return Pipeline.HandleValue(this, "_item/_stop", value);
      }
      public Object SendItemStop()
      {
         return SendItemStop(valueForItemStart);
      }
      public void OptSendItemStop()
      {
         if (itemStartPending) SendItemStop(valueForItemStart);
      }

   }

   public class MaxAddsExceededException : Exception
   {
      public readonly int Limit;
      public MaxAddsExceededException(int limit, String what)
         : base(String.Format("Max #{1} exceeded: {0}.", limit, what))
      {
         Limit = limit;
      }
      public MaxAddsExceededException(MaxAddsExceededException other)
         : base(other.Message, other)
      {
         Limit = other.Limit;
      }

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
