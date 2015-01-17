using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public class DatasourceReport: ISerializable
   {
      const String SER_NAME = "nm";
      const String SER_ADDED = "add";
      const String SER_DELETED = "del";
      const String SER_EMITTED = "emt";
      const String SER_ERRORS = "err";
      const String SER_STATUS = "stat";

      public String DatasourceName;
      public int Added, Emitted, Deleted, Errors;

      public DatasourceReport (PipelineContext ctx)
      {
         DatasourceName = ctx.DatasourceAdmin.Name;
         Added = ctx.Added;
         Deleted = ctx.Deleted;
         Emitted = ctx.Emitted;
         Errors = ctx.Errors;
      }
      public DatasourceReport(SerializationInfo info, StreamingContext ctxt)
      {
          //Get the values from info and assign them to the appropriate properties
         DatasourceName = (String)info.GetValue(SER_NAME, typeof(string));
         Added = (int)info.GetValue(SER_ADDED, typeof(int));
         Deleted = (int)info.GetValue(SER_DELETED, typeof(int));
         Emitted = (int)info.GetValue(SER_EMITTED, typeof(int));
         Errors = (int)info.GetValue(SER_ERRORS, typeof(int));
      }
        
      public void GetObjectData(SerializationInfo info, StreamingContext context)
      {
         info.AddValue(SER_NAME, DatasourceName);
         info.AddValue(SER_ADDED, Added);
         info.AddValue(SER_DELETED, Deleted);
         info.AddValue(SER_EMITTED, Emitted);
         info.AddValue(SER_ERRORS, Errors);
      }
   }
}
