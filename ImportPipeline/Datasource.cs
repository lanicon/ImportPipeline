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
      public Datasource Datasource {get; private set;}
      public Pipeline Pipeline { get; private set; }
      public String EndpointName { get; private set; }
      public int LogAdds { get; set; }
      public int MaxAdds { get; set; }
      public bool Active { get; private set; }

      public DatasourceAdmin(PipelineContext ctx, XmlNode node)
         : base(node)
      {
         Type = node.ReadStr("@type");
         Active = node.ReadBool("@active", true);
         LogAdds = node.ReadInt(1, "@logadds", -1);
         MaxAdds = node.ReadInt(1, "@maxadds", -1);
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
   }

}
