using Bitmanager.Elastic;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class ImportEngine
   {
      public XmlHelper Xml { get; private set; }
      public EndPoints EndPoints;
      public Converters Converters;
      public NamedAdminCollection<DatasourceAdmin> Datasources;
      public NamedAdminCollection<Pipeline> Pipelines;

      public void Load(String fileName)
      {
         XmlHelper xml = new XmlHelper(fileName);
         Load(xml);
      }
      public void Load(XmlHelper xml)
      {
         Xml = xml;
         PipelineContext ctx = new PipelineContext(this);

         EndPoints = new EndPoints(this, xml.SelectMandatoryNode("endpoints"));

         Converters = new Converters(
            xml.SelectSingleNode("converters"),
            "converter",
            (node) => Converter.Create (node),
            false);
         Pipelines = new NamedAdminCollection<Pipeline>(
            xml.SelectMandatoryNode("pipelines"),
            "pipeline",
            (node) => new Pipeline(this, node),
            true);
         Datasources = new NamedAdminCollection<DatasourceAdmin>(
            xml.SelectMandatoryNode("datasources"),
            "datasource",
            (node) => new DatasourceAdmin(ctx, node),
            true);
      }

      public void Import()
      {
         bool isError = true;
         EndPoints.Open(true);
         try
         {
            PipelineContext ctx = new PipelineContext(this);
            for (int i = 0; i < Datasources.Count; i++)
            {
               DatasourceAdmin admin = Datasources[i];
               if (!admin.Active)
               {
                  continue;
               }

               admin.Pipeline.Start(admin);
               admin.Datasource.Import(ctx, admin.Pipeline);
               admin.Pipeline.Stop(admin);
               isError = false;
            }
         }
         finally
         {
            EndPoints.Close(isError);
         }
      }
   }
}
