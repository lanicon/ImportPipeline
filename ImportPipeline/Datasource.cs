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
      bool   HandleException(PipelineContext ctx, String prefix, Exception err);
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
         String pipelineName = node.OptReadStr("@pipeline", null);
         Pipeline = ctx.ImportEngine.Pipelines.GetByNamesOrFirst(pipelineName, Name);

         //if (!Active) return; Zie notes: ws moet een datasource definitief kunnen worden uitgeschakeld. iets als active=true/false/disabled
         Datasource = ImportEngine.CreateObject<Datasource> (Type);
         Datasource.Init(ctx, node);
      }
   }

}
