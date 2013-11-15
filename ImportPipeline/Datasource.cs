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
      void HandleValue(PipelineContext ctx, String key, Object value);
   }

   public class DatasourceAdmin : NamedItem
   {
      public String Type { get; private set; }
      public Datasource Datasource {get; private set;}
      public Pipeline Pipeline { get; private set; }
      public bool Active { get; private set; }

      public DatasourceAdmin(PipelineContext ctx, XmlNode node)
         : base(node)
      {
         Type = node.ReadStr("@type");
         Active = node.OptReadBool("@active", true);
         String pipelineName = node.ReadStr("@pipeline");
         Pipeline = ctx.ImportEngine.Pipelines.GetByName(pipelineName);

         if (!Active) return;
         Datasource = createDatasource (Type);
         Datasource.Init(ctx, node);
      }

      private Datasource createDatasource(string type)
      {
         Datasource ret = PipelineContext.CreateObject(type) as Datasource;
         if (ret == null) throw new BMException("Datasource type={0} does not support IDatasource", type);

         return ret;
      }
   }

}
