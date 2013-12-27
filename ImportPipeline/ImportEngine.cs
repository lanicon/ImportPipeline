using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class ImportEngine
   {
      public XmlHelper Xml { get; private set; }
      public EndPoints EndPoints;
      public Converters Converters;
      public NamedAdminCollection<DatasourceAdmin> Datasources;
      public NamedAdminCollection<Pipeline> Pipelines;
      public readonly Logger ImportLog;
      public readonly Logger DebugLog;
      public readonly Logger ErrorLog;

      public ImportEngine()
      {
         ImportLog = Logs.CreateLogger("import", "ImportEngine");
         DebugLog = Logs.CreateLogger("import-debug", "ImportEngine");
         ErrorLog = Logs.ErrorLog.Clone("ImportEngine");
         Logs.DebugLog.Log(((InternalLogger)ImportLog)._Logger.Name);
      }
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

      static bool isActive(String[] enabledDSses, DatasourceAdmin da)
      {
         if (enabledDSses == null) return da.Active;
         for (int i = 0; i < enabledDSses.Length; i++)
         {
            if (da.Name.Equals(enabledDSses[i], StringComparison.InvariantCultureIgnoreCase)) return true;
         }
         return da.Active;
      }


      public void Import(String enabledDSses)
      {
         Import(enabledDSses.SplitStandard());
      }
      public void Import(String[] enabledDSses=null)
      {
         ImportLog.Log();
         ImportLog.Log(_LogType.ltProgress, "Starting import");
         bool isError = true;
         EndPoints.Open(true);
         ImportLog.Log(_LogType.ltProgress, "Endpoints opened");
         try
         {
            for (int i = 0; i < Datasources.Count; i++)
            {
               DatasourceAdmin admin = Datasources[i];
               if (!isActive(enabledDSses, admin))
               {
                  ImportLog.Log(_LogType.ltProgress, "[{0}]: not active", admin.Name);
                  continue;
               }


               ImportLog.Log(_LogType.ltProgress | _LogType.ltTimerStart, "[{0}]: starting import", admin.Name);
               admin.Pipeline.Start(admin);
               PipelineContext ctx = new PipelineContext(this, admin);
               admin.Datasource.Import(ctx, admin.Pipeline);
               admin.Pipeline.Stop(admin);
               ImportLog.Log(_LogType.ltProgress | _LogType.ltTimerStop, "[{0}]: import ended", admin.Name);
               isError = false;
            }
            ImportLog.Log(_LogType.ltProgress, "Import ended");
         }
         finally
         {
            EndPoints.Close(isError);
            foreach (var p in Pipelines)
            {
               p.Dump("after import");
            }

         }
      }

      private static String replaceKnownTypes(String typeName)
      {
         if (typeName != null)
         {
            switch (typeName.ToLowerInvariant())
            {
               case "endpoint": return typeof(EndPoint).FullName;
               case "esendpoint": return typeof(ESEndPoint).FullName;
               case "csv": return typeof(CsvDatasource).FullName;
            }
         }
         return typeName;
      }

      private static String replaceKnownTypes(XmlNode node)
      {
         return replaceKnownTypes (node.ReadStr("@type"));
      }
      public static T CreateObject<T>(String typeName) where T : class
      {
         return Objects.CreateObject<T>(replaceKnownTypes(typeName));
      }

      public static T CreateObject<T>(String typeName, params Object[] parms) where T : class
      {
         return Objects.CreateObject<T>(replaceKnownTypes(typeName), parms);
      }

      public static T CreateObject<T>(XmlNode node) where T : class
      {
         return Objects.CreateObject<T>(replaceKnownTypes(node));
      }

      public static T CreateObject<T>(XmlNode node, params Object[] parms) where T : class
      {
         return Objects.CreateObject<T>(replaceKnownTypes(node), parms);
      }
   }
}
