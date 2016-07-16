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
      private _ErrorState errorState;

      public ImportReport()
      {
         DatasourceReports = new List<DatasourceReport>();
      }

      public void Add(DatasourceReport rep)
      {
         errorState |= rep.ErrorState; 
         //if (rep.Errors > 0 || rep.ErrorMessage != null)
         //   hasErrors = true;
         DatasourceReports.Add(rep);
      }

      public _ErrorState ErrorState { get { return errorState; } }

      public void SetGlobalStatus(PipelineContext ctx)
      {
         ErrorMessage = ctx.LastError == null ? null : ctx.LastError.Message;
      }

      public override string ToString()
      {
         var sb = new LeveledStringBuilder("-- ", "   ");
         foreach (var ds in DatasourceReports)
            ds.ToString(sb.OptAppendLine());
         return sb.ToString();
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
      public List<PostProcessorReport> PostProcessorReports;
      public int Added, Emitted, Deleted, Errors, Skipped, ElapsedSeconds;
      public String Stats;
      public _ErrorState ErrorState;
      private DateTime utcStart;

      public DatasourceReport(DatasourceAdmin ds)
      {
         utcStart = DateTime.UtcNow;
         DatasourceName = ds.Name;
         ErrorState = _ErrorState.Running;
         Stats = "Running...";
      }
      public void MarkEnded(PipelineContext ctx)
      {
         ElapsedSeconds = (int)(DateTime.UtcNow - utcStart).TotalSeconds;
         Added = ctx.Added;
         Deleted = ctx.Deleted;
         Emitted = ctx.Emitted;
         Errors = ctx.Errors;
         Skipped = ctx.Skipped;
         ErrorMessage = ctx.LastError == null ? null : ctx.LastError.Message;

         StringBuilder sb = new StringBuilder();
         sb.Append("Elapsed=");
         sb.Append(Pretty.PrintElapsed(ElapsedSeconds));
         sb.Append(", ");
         sb.Append(ctx.GetStats());
         Stats = sb.ToString();

         ErrorState = ctx.ErrorState == _ErrorState.Running ? _ErrorState.OK : ctx.ErrorState;
      }

      public override string ToString()
      {
         var sb = new LeveledStringBuilder("   ");
         ToString(sb, true);
         return sb.ToString();
      }
      public LeveledStringBuilder ToString(LeveledStringBuilder sb, bool withName=true)
      {
         if (withName)
         {
            sb.Append(DatasourceName);
            sb.Append(": ");
         }
         sb.Append(Stats);
         sb++;
         if (ErrorMessage != null)
            sb.AppendLine().Append(ErrorMessage);

         if (PostProcessorReports != null)
         {
            sb++;
            foreach (var ppr in PostProcessorReports)
            {
               sb.AppendLine();
               ppr.ToString(sb);
            }
            sb--;
         }
         sb--;
         return sb;
      }

      public void AddPostProcessorReport (PostProcessorReport rep)
      {
         if (PostProcessorReports == null) PostProcessorReports = new List<PostProcessorReport>();
         PostProcessorReports.Add(rep);
      }
   }


   /// <summary>
   /// Holds global stats about a postprocessor
   /// </summary>
   [Serializable]
   public class PostProcessorReport
   {
      public String Name;
      public int Received, Passed, Skipped, ElapsedSeconds;
      public String Stats;
      private DateTime utcStart;

      public PostProcessorReport(IPostProcessor proc)
      {
         utcStart = DateTime.UtcNow;
         Name = proc.Name;
         Stats = "Running...";
      }
      public void MarkEnded(PipelineContext ctx)
      {
         ElapsedSeconds = (int)(DateTime.UtcNow - utcStart).TotalSeconds;
         StringBuilder sb = new StringBuilder();
         sb.Append("Elapsed=");
         sb.Append(Pretty.PrintElapsed(ElapsedSeconds));
         sb.Append(", ");
         sb.AppendFormat ("In={0}, Out={1}, Skipped={2}.", Received, Passed, Skipped);
         Stats = sb.ToString();
      }

      public override string ToString()
      {
         return Name + ": " + Stats;
      }

      public LeveledStringBuilder ToString(LeveledStringBuilder sb, bool withName = true)
      {
         if (withName)
            sb.Append(Name).Append(": ");
         sb.Append(Stats);
         return sb;
      }
   }
}
