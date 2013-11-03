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
      void Init(XmlNode node);
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

      public DatasourceAdmin(ImportEngine engine, XmlNode node)
         : base(node)
      {
         Type = node.ReadStr("@type");
         Active = node.OptReadBool("@active", true);
         String pipelineName = node.ReadStr("@pipeline");
         Pipeline = engine.Pipelines.GetByName(pipelineName);

         if (!Active) return;
         Datasource = createDatasource (Type);
         Datasource.Init(node);
      }

      private Datasource createDatasource(string type)
      {
         Object ds;
         switch (type.ToLowerInvariant())
         {
            case "csv": ds = new CsvDatasource(); break;
            default:
               throw new BMException ("Invalid datasource type: {0}.", type); 
         }

         Datasource ret = ds as Datasource;
         if (ret == null) throw new BMException("Datasource type={0} (.Net type={1}) does not support IDatasource", type, ds.GetType().FullName);

         return ret;
      }
   }

}
