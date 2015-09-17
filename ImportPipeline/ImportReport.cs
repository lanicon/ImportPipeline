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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Holds global stats about an import
   /// </summary>
   [Serializable]
   public class ImportReport
   {
      public List<DatasourceReport> DatasourceReports;
      public String ErrorMessage;
      private bool hasErrors;

      public ImportReport()
      {
         DatasourceReports = new List<DatasourceReport>();
      }

      public void Add(DatasourceReport rep)
      {
         if (rep.Errors > 0 || rep.ErrorMessage != null)
            hasErrors = true;
         DatasourceReports.Add(rep);
      }

      public bool HasErrors { get { return hasErrors; } }

      public void SetGlobalStatus(PipelineContext ctx)
      {
         ErrorMessage = ctx.LastError == null ? null : ctx.LastError.Message;
      }
   }

   public class Pretty
   {
      public static String ToElapsed (int seconds)
      {
         if (seconds < 60) return String.Format("{0} sec", seconds);
         int min = seconds / 60;
         seconds = seconds % 60;
         if (min < 60) return String.Format("{0:00}:{1:00}", min, seconds);

         int h = min / 60;
         min = min % 60;
         return String.Format("{0:00}:{1:00}:{2:00}", h, min, seconds);
      }
   }
   /// <summary>
   /// Holds global stats about a datasource
   /// </summary>
   [Serializable]
   public class DatasourceReport
   {
      public String DatasourceName;
      public String ErrorMessage;
      public int Added, Emitted, Deleted, Errors, Skipped, PostProcessed, ElapsedSeconds;
      public String Stats;

      public DatasourceReport(PipelineContext ctx, DateTime utcStart)
      {
         ElapsedSeconds = (int)(DateTime.UtcNow - utcStart).TotalSeconds;
         DatasourceName = ctx.DatasourceAdmin.Name;
         Added = ctx.Added;
         Deleted = ctx.Deleted;
         Emitted = ctx.Emitted;
         Errors = ctx.Errors;
         Skipped = ctx.Skipped;
         PostProcessed = ctx.PostProcessed;
         ErrorMessage = ctx.LastError == null ? null : ctx.LastError.Message;
         Stats = ctx.GetStats() + ", elapsed="+Pretty.ToElapsed(ElapsedSeconds);
      }

      public String GetStats()
      {
         return Stats;// String.Format("Emitted={3}, Added={0}, Skipped={2}, Errors={4}, Deleted={1}", Added, Deleted, Skipped, Emitted, Errors);
      }


      public override string ToString()
      {
         if (ErrorMessage == null)
            return DatasourceName + "\t " + GetStats();
         return DatasourceName + "\t " + GetStats() + "\r\n\t" + ErrorMessage;
      }
   }
}
