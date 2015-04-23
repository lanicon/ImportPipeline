using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline
{
   public interface Datasource
   {
      void Init(PipelineContext ctx, XmlNode node);
      void Import(PipelineContext ctx, IDatasourceSink sink);
   }
   public interface IDatasourceSink
   {
      Object HandleValue(PipelineContext ctx, String key, Object value);
   }

   public class DatasourceAdmin : NamedItem
   {
      public String Type { get; private set; }
      public int ShiftLastRuntime {get; set;}
      public Datasource Datasource {get; private set;}
      public Pipeline Pipeline { get; private set; }
      public String EndpointName { get; private set; }
      public int LogAdds { get; set; }
      public int MaxAdds { get; set; }
      public int MaxEmits { get; set; }
      public bool Active { get; private set; }

      public DatasourceAdmin(PipelineContext ctx, XmlNode node)
         : base(node)
      {
         Type = node.ReadStr("@type");
         Active = node.ReadBool("@active", true);
         LogAdds = node.ReadInt(1, "@logadds", -1);
         MaxAdds = node.ReadInt(1, "@maxadds", -1);
         MaxEmits = node.ReadInt(1, "@maxemits", -1);
         String tmp = node.ReadStr(1, "@shiftlastruntime", null);
         ShiftLastRuntime = computeRuntimeShift (tmp);
         if (ShiftLastRuntime == int.MinValue) throw new BMNodeException(node, "Invalid shiftlastruntime [{0}]: must be <int>[d|h|m|s].", tmp);

         String pipelineName = node.ReadStr(1, "@pipeline", null);
         Pipeline = ctx.ImportEngine.Pipelines.GetByNamesOrFirst(pipelineName, Name);

         String endpoint = node.ReadStr(1, "@endpoint", null);
         if (endpoint != null)
         {
            endpoint = endpoint.Replace("*", Name);
            ctx.ImportEngine.Endpoints.CheckDataEndpoint(ctx, endpoint, true);
            EndpointName = endpoint;
         }
         String endpointExpr = node.ReadStr(1, "@endpoint_expr", null);
         if (endpointExpr != null)
         {
            if (endpoint != null) throw new BMNodeException(node, "@endpoint and @endpoint_expr cannot be specified both.");
            EndpointName = PerlRegex.Replace(endpointExpr, Name);
            ctx.ImportEngine.Endpoints.CheckDataEndpoint(ctx, EndpointName, true);
         }

         Pipeline.CheckEndpoints(ctx, this);


         //if (!Active) return; Zie notes: ws moet een datasource definitief kunnen worden uitgeschakeld. iets als active=true/false/disabled
         Datasource = ImportEngine.CreateObject<Datasource> (Type);
         Datasource.Init(ctx, node);
      }

      public void Import (PipelineContext ctx)
      {
         Logger importLog = ctx.ImportLog;
         Logger errorLog = ctx.ErrorLog;
         bool stopNeeded = false;
         importLog.Log(_LogType.ltProgress | _LogType.ltTimerStart, "[{0}]: starting import with pipeline {1}, default endpoint={2}, maxadds={3} ", Name, Pipeline.Name, Pipeline.DefaultEndpoint, ctx.MaxAdds);

         Pipeline.Start(ctx);
         Pipeline.HandleValue(ctx, "_datasource/_start", this);
         try
         {
            Datasource.Import(ctx, Pipeline);
            importLog.Log(_LogType.ltProgress | _LogType.ltTimerStop, "[{0}]: import ended. {1}.", Name, ctx.GetStats());
            stopNeeded = true;
         }

         catch (Exception e)
         {
            ctx.LastError = e;
            if (MaxAddsExceededException.ContainsMaxAddsExceededException(e))
            {
               stopNeeded = true;
               ctx.ErrorState |= _ErrorState.Limited;
               importLog.Log(_LogType.ltWarning | _LogType.ltTimerStop, "[{0}]: {1}", Name, e.Message);
               importLog.Log("-- " + ctx.GetStats());
               if ((ctx.ImportFlags & _ImportFlags.IgnoreLimited) != 0)
                  importLog.Log(_LogType.ltWarning, "Limited ignored due to importFlags [{0}].", ctx.ImportFlags);
               stopNeeded = true;
            }
            else
            {
               ctx.ErrorState |= _ErrorState.Error;
               String msg = String.Format("[{0}]: crashed err={1}", Name, e.Message);
               importLog.Log(_LogType.ltError | _LogType.ltTimerStop, msg);
               importLog.Log("-- " + ctx.GetStats());
               errorLog.Log(_LogType.ltError, msg);
               ctx.ErrorLog.Log(e);
               if ((ctx.ImportFlags & _ImportFlags.IgnoreErrors) == 0)
                  throw new BMException(e, "{0}\r\nDatasource={1}.", e.Message, Name);

               msg = String.Format("Exception ignored due to importFlags [{0}].", ctx.ImportFlags);
               importLog.Log(_LogType.ltWarning, msg);
               errorLog.Log(_LogType.ltWarning, msg);
               stopNeeded = true;
            }
         }

         finally
         {
            try
            {
               if (stopNeeded)
               {
                  ctx.OptSendItemStop();
                  Pipeline.HandleValue(ctx, "_datasource/_stop", this);
               }
            }
            finally
            {
               Pipeline.Stop(ctx);
            }
         }
      }

      static int computeRuntimeShift (String x)
      {
         if (String.IsNullOrEmpty(x)) return 0;
         int mult = 1;
         switch (x[x.Length-1])
         {
            case 'd':
            case 'D':
               mult = 3600*24;
               x = x.Substring(0, x.Length - 1);
               break;

            case 'h':
            case 'H':
               mult = 3600;
               x = x.Substring(0, x.Length - 1);
               break;

            case 'm':
            case 'M':
               mult = 60;
               x = x.Substring (0, x.Length-1);
               break;

            case 's':
            case 'S':
               mult = 1;
               x = x.Substring (0, x.Length-1);
               break;
         }
         int v;
         return (int.TryParse (x, out v)) ? mult * v : int.MinValue;
      }
   }

}
