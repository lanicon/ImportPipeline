﻿using System;
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

      public ImportReport()
      {
         DatasourceReports = new List<DatasourceReport>();
      }

      public void Add(DatasourceReport rep)
      {
         DatasourceReports.Add(rep);
      }

      public void SetGlobalStatus(PipelineContext ctx)
      {
         ErrorMessage = ctx.LastError == null ? null : ctx.LastError.Message;
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
      public int Added, Emitted, Deleted, Errors, Skipped;

      public DatasourceReport(PipelineContext ctx)
      {
         DatasourceName = ctx.DatasourceAdmin.Name;
         Added = ctx.Added;
         Deleted = ctx.Deleted;
         Emitted = ctx.Emitted;
         Errors = ctx.Errors;
         Skipped = ctx.Skipped;
         ErrorMessage = ctx.LastError == null ? null : ctx.LastError.Message;
      }

      public String GetStats()
      {
         return String.Format("Emitted={3}, Added={0}, Skipped={2}, Errors={4}, Deleted={1}", Added, Deleted, Skipped, Emitted, Errors);
      }


      public override string ToString()
      {
         if (ErrorMessage == null)
            return DatasourceName + "\t " + GetStats();
         return DatasourceName + "\t " + ErrorMessage;
      }
   }
}