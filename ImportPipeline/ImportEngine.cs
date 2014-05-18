using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   [Flags]
   public enum _ImportFlags
   {
      ImportFull = 1 << 0,
      FullImport = 1 << 0,
      DoNotRename = 1 << 1,
      TraceValues = 1<<2,
      IgnoreErrors = 1<<3,
      IgnoreLimited = 1<<4,
      IgnoreAll = IgnoreErrors | IgnoreLimited,
      UseFlagsFromXml = 1 << 5,
      Silent = 1<<6,
   }
   public class ImportEngine
   {
      public XmlHelper Xml { get; private set; }
      public Endpoints Endpoints;
      public Converters Converters;
      public NamedAdminCollection<DatasourceAdmin> Datasources;
      public NamedAdminCollection<Pipeline> Pipelines;
      public ProcessHostCollection JavaHostCollection;
      public ScriptHost ScriptHost;
      public readonly Logger ImportLog;
      public readonly Logger DebugLog;
      public readonly Logger ErrorLog;
      public readonly Logger MissedLog;
      public int LogAdds {get; private set;}
      public int MaxAdds { get; private set; }
      public _ImportFlags ImportFlags { get; set; }


      public ImportEngine()
      {
         ImportLog = Logs.CreateLogger("import", "ImportEngine");
         DebugLog = Logs.CreateLogger("import-debug", "ImportEngine");
         MissedLog = Logs.CreateLogger("import-missed", "ImportEngine");
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
         ImportFlags = xml.OptReadEnum("@importflags", ImportFlags);
         LogAdds = xml.OptReadInt("@logadds", 50000);
         MaxAdds = xml.OptReadInt("@maxadds", -1);
         ImportLog.Log("Loading import xml: flags={0}, logadds={1}, maxadds={2}", ImportFlags, LogAdds, MaxAdds);

         //Load the supplied script
         ImportLog.Log(_LogType.ltTimerStart, "loading: scripts"); 
         XmlNode scriptNode = xml.SelectSingleNode("script");
         if (scriptNode != null)
         {
            ScriptHost = new ScriptHost();
            String fn = xml.CombinePath (scriptNode.ReadStr("@file"));
            ScriptHost.AddFile(fn);
            ScriptHost.AddReference(Assembly.GetExecutingAssembly());
            ScriptHost.Compile();
         }

         ImportLog.Log(_LogType.ltTimer, "loading: helper process definitions ");
         JavaHostCollection = new ProcessHostCollection(this, xml.SelectSingleNode("processes"));

         ImportLog.Log(_LogType.ltTimer, "loading: endpoints");
         Endpoints = new Endpoints(this, xml.SelectMandatoryNode("endpoints"));

         ImportLog.Log(_LogType.ltTimer, "loading: converters");
         Converters = new Converters(
            xml.SelectSingleNode("converters"),
            "converter",
            (node) => Converter.Create (node),
            false);

         ImportLog.Log(_LogType.ltTimer, "loading: pipelines");
         Pipelines = new NamedAdminCollection<Pipeline>(
            xml.SelectMandatoryNode("pipelines"),
            "pipeline",
            (node) => new Pipeline(this, node),
            true);
         
         ImportLog.Log(_LogType.ltTimer, "loading: datasources");
         Datasources = new NamedAdminCollection<DatasourceAdmin>(
            xml.SelectMandatoryNode("datasources"),
            "datasource",
            (node) => new DatasourceAdmin(ctx, node),
            true);
      
         ImportLog.Log(_LogType.ltTimerStop, "loading: finished");
      }

      static bool isActive(String[] enabledDSses, DatasourceAdmin da)
      {
         if (enabledDSses == null) return da.Active;
         for (int i = 0; i < enabledDSses.Length; i++)
         {
            if (da.Name.Equals(enabledDSses[i], StringComparison.InvariantCultureIgnoreCase)) return true;
         }
         return false;
      }


      public void Import(String enabledDSses)
      {
         Import(enabledDSses.SplitStandard());
      }
      public void Import(String[] enabledDSses=null)
      {
         ImportLog.Log();
         ImportLog.Log(_LogType.ltProgress, "Starting import. Flags={0}, ActiveDS's='{1}'.", ImportFlags, enabledDSses==null ? null : String.Join (", ", enabledDSses));
         PipelineContext mainCtx = new PipelineContext(this);
         Endpoints.Open(mainCtx);

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

               PipelineContext ctx = new PipelineContext(this, admin);
               ImportLog.Log(_LogType.ltProgress | _LogType.ltTimerStart, "[{0}]: starting import with pipeline {1}, default endpoint={2}, maxadds={3} ", admin.Name, admin.Pipeline.Name, admin.Pipeline.DefaultEndpoint, ctx.MaxAdds);
               try
               {
                  admin.Import(ctx);
                  ImportLog.Log(_LogType.ltProgress | _LogType.ltTimerStop, "[{0}]: import ended. {1}.", admin.Name, ctx.GetStats());
               }
               catch (Exception err)
               {
                  if (MaxAddsExceededException.ContainsMaxAddsExceededException (err))
                  {
                     ctx.ErrorState |= _ErrorState.Limited;
                     ImportLog.Log(_LogType.ltWarning | _LogType.ltTimerStop, "[{0}]: {1}", admin.Name, err.Message);
                     ImportLog.Log("-- " + ctx.GetStats());
                     if ((ImportFlags & _ImportFlags.IgnoreLimited) != 0)
                        ImportLog.Log(_LogType.ltWarning, "Limited ignored due to importFlags [{0}].", ImportFlags);
                     else
                        mainCtx.ErrorState |= _ErrorState.Limited;
                  }
                  else
                  {
                     ctx.ErrorState |= _ErrorState.Error;
                     ImportLog.Log(_LogType.ltError | _LogType.ltTimerStop, "[{0}]: crashed err={1}", admin.Name, err.Message);
                     ImportLog.Log("-- " + ctx.GetStats());
                     Exception toThrow = new BMException(err, "{0}\r\nDatasource={1}\r\nLastAction={2}.", err.Message, admin.Name, admin.Pipeline.LastAction);
                     ErrorLog.Log(toThrow);
                     if ((ImportFlags & _ImportFlags.IgnoreErrors) != 0)
                        ImportLog.Log(_LogType.ltWarning, "Error ignored due to importFlags [{0}].", ImportFlags);
                     else
                     {
                        mainCtx.ErrorState |= _ErrorState.Error;
                        throw toThrow;
                     }
                  }
               }
               Endpoints.OptClosePerDatasource(ctx);

               foreach (var c in Converters) c.DumpMissed(ctx);
            }
            ImportLog.Log(_LogType.ltProgress, "Import ended");
            JavaHostCollection.StopAll();
         }
         finally
         {
            try
            {
               Endpoints.Close(mainCtx);
               JavaHostCollection.StopAll();
            }
            catch (Exception e2)
            {
               ErrorLog.Log(e2);
               ImportLog.Log(e2);
            }
         }
      }

      private static String replaceKnownTypes(String typeName)
      {
         if (typeName != null)
         {
            switch (typeName.ToLowerInvariant())
            {
               case "endpoint": return typeof(Endpoint).FullName;
               case "esendpoint": return typeof(ESEndpoint).FullName;
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
