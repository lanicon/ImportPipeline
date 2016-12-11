using Bitmanager.Elastic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public static class ESHelper
   {
      public static ESConnection CreateConnection(PipelineContext ctx, String url)
      {
         return SetLogging(ctx, new ESConnection(url));
      }
      public static ESConnection SetLogging(PipelineContext ctx, ESConnection conn)
      {
         ESConnection._Logging flags = 0;
         if (ctx.Switches.IsOn("es.log"))
            flags |= ESConnection._Logging.LogRequest | ESConnection._Logging.LogResponse;
         if (ctx.Switches.IsOn("es.logrequest"))
            flags |= ESConnection._Logging.LogRequest;
         if (ctx.Switches.IsOn("es.logresponse"))
            flags |= ESConnection._Logging.LogResponse;
         conn.Logging = flags;
         return conn;
      }
   }
}
