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
using Bitmanager.IO;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{

   public class RunAdministration
   {
      public const String ADM_DATE = "adm_date";
      public const String ADM_DS = "adm_ds";
      public const String ADM_FLAGS = "adm_flags";
      public const String ADM_STATE = "adm_state";
      public const String ADM_ADDED = "adm_added";
      public const String ADM_DELETED = "adm_deleted";
      public const String ADM_SKIPPED = "adm_skipped";
      public const String ADM_EMITTED = "adm_emitted";
      public const String ADM_ERRORS = "adm_errors";

      public readonly String Key;
      public readonly DateTime RunDateUtc;
      public readonly String DataSource;
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

      public RunAdministration(JObject obj)
      {
         RunDateUtc = obj.ReadDate (ADM_DATE);
         DataSource = obj.ReadStr(ADM_DS);
         ImportFlags = Invariant.ToEnum(obj.ReadStr(ADM_FLAGS), (_ImportFlags)0);
         State = Invariant.ToEnum(obj.ReadStr(ADM_STATE), _ErrorState.Error);
         Added = obj.ReadInt(ADM_ADDED);
         Deleted = obj.ReadInt(ADM_DELETED);
         Skipped = obj.ReadInt(ADM_SKIPPED);
         Emitted = obj.ReadInt(ADM_EMITTED);
         Errors = obj.ReadInt(ADM_ERRORS);
      }

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

      public override string ToString()
      {
         return String.Format("ADM[{0}-{1}: flags={2} state={3}]", DataSource, RunDateUtc, ImportFlags, State);
      }


   }

}
