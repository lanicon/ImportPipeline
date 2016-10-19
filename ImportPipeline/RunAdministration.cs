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
using Bitmanager.Elastic;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class RunAdministration
   {
      public DateTime RunDateUtc;
      public String DataSource;
      public int Added, Deleted, Skipped, Emitted, Errors;

      public _ErrorState State;
      public _ImportFlags ImportFlags;

      public RunAdministration(PipelineContext ctx)
      {
         RunDateUtc = ctx.NewLastUpdated;
         DataSource = ctx.DatasourceAdmin.Name;
         State = ctx.ErrorState;
         Added = ctx.Added;
         Deleted = ctx.Deleted;
         Skipped = ctx.Skipped;
         Emitted = ctx.Emitted;
         Errors = ctx.Errors;
         ImportFlags = ctx.ImportFlags;
      }

      public RunAdministration(GenericDocument obj)
      {
         FromJson(obj);
      }

      public const String ADM_DATE = "adm_date";
      public const String ADM_DS = "adm_ds";
      public const String ADM_FLAGS = "adm_flags";
      public const String ADM_STATE = "adm_state";
      public const String ADM_ADDED = "adm_added";
      public const String ADM_DELETED = "adm_deleted";
      public const String ADM_SKIPPED = "adm_skipped";
      public const String ADM_EMITTED = "adm_emitted";
      public const String ADM_ERRORS = "adm_errors";

      public JObject ToJson()
      {
         JObject ret = new JObject();
         ret[ADM_DATE] = RunDateUtc;
         ret[ADM_DS] = DataSource;
         ret[ADM_STATE] = State.ToString();
         ret[ADM_FLAGS] = ImportFlags==0 ? String.Empty : ImportFlags.ToString();
         ret[ADM_ADDED] = Added;
         ret[ADM_DELETED] = Deleted;
         ret[ADM_SKIPPED] = Skipped;
         ret[ADM_EMITTED] = Emitted;
         ret[ADM_ERRORS] = Errors;
         return ret;
      }

      public void FromJson(GenericDocument obj)
      {
         RunDateUtc = (DateTime)obj.GetField(ADM_DATE);
         DataSource = (String)obj.GetField(ADM_DS);
         ImportFlags = Invariant.ToEnum((String)obj.GetField(ADM_FLAGS), (_ImportFlags)0);
         State = Invariant.ToEnum((String)obj.GetField(ADM_STATE), _ErrorState.Error);
         Added = (int)obj.GetField(ADM_ADDED);
         Deleted = (int)obj.GetField(ADM_DELETED);
         Skipped = (int)obj.GetField(ADM_SKIPPED);
         Emitted = (int)obj.GetField(ADM_EMITTED);
         Errors = (int)obj.GetField(ADM_ERRORS);
      }

      public override string ToString()
      {
         return String.Format("ADM[{0}-{1}: flags={2} state={3}]", DataSource, RunDateUtc, ImportFlags, State);
      }
   }

   public static class RunAdministrationExt
   {
      public static RunAdministration GetLastOKRun(this List<RunAdministration> list, String ds)
      {
         if (list == null || list.Count==0) return null;
         RunAdministration ret = null;
         foreach (var a in list)
         {
            if (!String.Equals(a.DataSource, ds, StringComparison.OrdinalIgnoreCase)) continue;
            if (a.State != _ErrorState.OK) continue;
            if (ret == null || ret.RunDateUtc < a.RunDateUtc)
               ret = a;
         }
         return ret;
      }
      public static DateTime GetLastOKRunDate(this List<RunAdministration> list, DatasourceAdmin ds)
      {
         var a = GetLastOKRun(list, ds.Name);
         return a == null ? DateTime.MinValue : a.RunDateUtc;
      }
      public static DateTime GetLastOKRunDateShifted(this List<RunAdministration> list, DatasourceAdmin ds)
      {
         var a = GetLastOKRun(list, ds.Name);
         return a == null ? DateTime.MinValue : a.RunDateUtc.AddSeconds(+ds.ShiftLastRuntime);
      }
   }
}
