using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.IO;
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
      Silent = 1 << 6,
      RetryErrors = 1 << 7,
      MaxAddsToMaxEmits = 1 << 8,
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
      public Logger ImportLog;
      public Logger DebugLog;
      public Logger ErrorLog;
      public Logger MissedLog;
      public DateTime StartTimeUtc { get; private set; }
      public int LogAdds { get; set; }
      public int MaxAdds { get; set; }
      public int MaxEmits { get; set; }
      public _ImportFlags ImportFlags { get; set; }


      public ImportEngine()
      {
         createLogs();
         LogAdds = 50000;
         MaxAdds = -1;
         MaxEmits = -1;
      }

      private void createLogs()
      {
         ImportLog = Logs.CreateLogger("import", "ImportEngine");
         DebugLog = Logs.CreateLogger("import-debug", "ImportEngine");
         MissedLog = Logs.CreateLogger("import-missed", "ImportEngine");
         ErrorLog = Logs.ErrorLog.Clone("ImportEngine");
         Logs.DebugLog.Log(((InternalLogger)ImportLog)._Logger.Name);
         ImportLog.Log();
         ErrorLog.Log();
      }

      public void Load(String fileName)
      {
         if (!File.Exists(fileName))
         {
            String tmp = Path.Combine("ImportDirs", fileName);
            if (File.Exists(tmp)) fileName = tmp;
         }
         String dir = Path.GetDirectoryName(Path.GetFullPath(fileName));
         dir = Path.Combine(dir, "logs");
         if (Directory.Exists(dir))
         {
            LogFactorySettings.Instance.AppLogPath = dir;
            createLogs();
         }

         XmlHelper xml = new XmlHelper(fileName);
         Load(xml);
      }

      public void Load(XmlHelper xml)
      {
         Xml = xml;
         String dir = xml.FileName;
         if (!String.IsNullOrEmpty(dir)) dir = Path.GetDirectoryName(xml.FileName);
         Environment.SetEnvironmentVariable("IMPORT_DIR", dir);
         fillTikaVars();

         PipelineContext ctx = new PipelineContext(this);
         ImportFlags = xml.ReadEnum("@importflags", ImportFlags);
         LogAdds = xml.ReadInt("@logadds", LogAdds);
         MaxAdds = xml.ReadInt("@maxadds", MaxAdds);
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

      private void fillTikaVars()
      {
         String dir = IOUtils.FindDirectoryToRoot(Assembly.GetExecutingAssembly().Location, "TikaService", FindToTootFlags.ReturnNull);
         if (String.IsNullOrEmpty(dir)) return;
         Environment.SetEnvironmentVariable("IMPORT_TIKA_SERVICE_DIR", dir);

         //String jetty = findLargest(dir, "jetty-runner-*.jar");
         //if (jetty == null) return;

         //String war = findLargest(Path.Combine(dir, "target"), "tikaservice-*.war");
         //if (war == null) return;

         //Environment.SetEnvironmentVariable("IMPORT_TIKA_CMD", String.Format("\"{0}\"  \"{1}\"", jetty, war));

         String srv = findLargest(dir, "tikaservice-*.jar");
         if (srv == null) srv = findLargest(Path.Combine(dir, "target"), "tikaservice-*.jar");
         Environment.SetEnvironmentVariable("IMPORT_TIKA_CMD", "\"" + srv + "\"");
         Environment.SetEnvironmentVariable("IMPORT_TIKA_URLBASE", "http://localhost:8080");
      }

      private String findLargest(String dir, String spec)
      {
         String max = null;
         String[] files = Directory.GetFiles(dir, spec);
         foreach (var f in files)
         {
            if (String.Compare(f, max, true) <= 0) continue;
            max = f;
         }
         return max;
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


      public ImportReport Import(String enabledDSses)
      {
         return Import(enabledDSses.SplitStandard());
      }
      public ImportReport Import(String[] enabledDSses = null)
      {
         var ret = new ImportReport();
         StartTimeUtc = DateTime.UtcNow;

         ImportLog.Log();
         ImportLog.Log(new String ('_', 80));
         ImportLog.Log(_LogType.ltProgress, "Starting import. Flags={0}, MaxAdds={1}, ActiveDS's='{2}'.", ImportFlags, MaxAdds, enabledDSses == null ? null : String.Join(", ", enabledDSses));
         PipelineContext mainCtx = new PipelineContext(this);
         Endpoints.Open(mainCtx);

         try
         {
            _ErrorState stateFilter = ((ImportFlags & _ImportFlags.IgnoreLimited) != 0) ? (~_ErrorState.Limited) : _ErrorState.All; 

            for (int i = 0; i < Datasources.Count; i++)
            {
               DatasourceAdmin admin = Datasources[i];
               if (!isActive(enabledDSses, admin))
               {
                  ImportLog.Log(_LogType.ltProgress, "[{0}]: not active", admin.Name);
                  continue;
               }

               PipelineContext ctx = new PipelineContext(this, admin);
               var pipeline = admin.Pipeline;

               try
               {
                  admin.Import(ctx);
                  mainCtx.ErrorState |= (ctx.ErrorState & stateFilter);
                  if (ctx.LastError != null) mainCtx.LastError = ctx.LastError;
                  ret.Add(new DatasourceReport(ctx));
               }
               catch (Exception err)
               {
                  mainCtx.LastError = err;
                  mainCtx.ErrorState |= (ctx.ErrorState & stateFilter) | _ErrorState.Error;
                  throw;
               }
               Endpoints.OptClosePerDatasource(ctx);

               foreach (var c in Converters) c.DumpMissed(ctx);
            }
            ImportLog.Log(_LogType.ltProgress, "Import ended");
            JavaHostCollection.StopAll();
            Endpoints.Close(mainCtx);
         }
         catch (Exception err2)
         {
            mainCtx.LastError = err2;
            throw;
         }
         finally
         {
            try
            {
               Endpoints.CloseFinally(mainCtx);
               JavaHostCollection.StopAll();
            }
            catch (Exception e2)
            {
               ErrorLog.Log(e2);
               ImportLog.Log(e2);
            }
         }
         ret.SetGlobalStatus(mainCtx);
         return ret;
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
               case "sql": return typeof(SqlDatasource).FullName;
               case "mysql": return typeof(MysqlDatasource).FullName;
               case "odbc": return typeof(OdbcDatasource).FullName;
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
