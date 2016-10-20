/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.ImportPipeline.Template;
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
      LogEmits = 1 << 9,
      /// <summary>
      /// Dump the generated template output
      /// </summary>
      DebugTemplate = 1 << 11,
      NoMailReport = 1 << 12
   }
   public class ImportEngine: IDisposable
   {
      public XmlHelper Xml { get; private set; }
      public Endpoints Endpoints;
      public Converters Converters;
      public NamedAdminCollection<DatasourceAdmin> Datasources;
      public NamedAdminCollection<Pipeline> Pipelines;
      public NamedAdminCollection<CategoryCollection> Categories;
      public ProcessHostCollection ProcessHostCollection;
      public PostProcessors PostProcessors;
      public ScriptHost ScriptHost;
      protected internal ScriptExpressionHolder ScriptExpressions;
      public Logger ImportLog;
      public Logger DebugLog;
      public Logger ErrorLog;
      public Logger MissedLog;
      public DateTime StartTimeUtc { get; private set; }
      public int LogAdds { get; set; }
      public int MaxAdds { get; set; }
      public int MaxEmits { get; set; }
      public _ImportFlags ImportFlags { get; set; }
      public String OverrideEndpoint { get; set; }
      public String OverridePipeline { get; set; }
      private String binDir;
      private MailReporter Reporter;

      public ImportEngine()
      {
         createLogs();
         LogAdds = 50000;
         MaxAdds = -1;
         MaxEmits = -1;
         ImportFlags = _ImportFlags.UseFlagsFromXml;
         AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
      }

      public void Dispose()
      {
         AppDomain.CurrentDomain.AssemblyResolve -= CurrentDomain_AssemblyResolve;
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

      private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
      {
         if (binDir == null) return null;
         String fn = Path.Combine(binDir, args.Name);
         if (!File.Exists(fn)) fn = fn + ".dll";
         ImportLog.Log(_LogType.ltInfo, "Assembly '{0}' not resolved. Trying '{1}'...", args.Name, fn);
         try
         {
            return Assembly.LoadFrom(fn);
         }
         catch (Exception err)
         {
            Logs.ErrorLog.Log(err);
            return null;
         }
      }

      public void Load(String fileName)
      {
         fileName = locateFile(fileName);
         String dir = Path.GetDirectoryName(Path.GetFullPath(fileName));
         dir = Path.Combine(dir, "logs");
         if (Directory.Exists(dir))
         {
            LogFactorySettings.Instance.AppLogPath = dir;
            createLogs();
         }

         Load(loadXml (fileName));
      }

      String locateFile (String file)
      {
         if (File.Exists(file)) goto EXIT_RTN;

         String org = file;
         String dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
         file = Path.Combine(dir, org);
         if (File.Exists(file)) goto EXIT_RTN;

         dir = IOUtils.FindDirectoryToRoot(dir, "ImportDirs");
         if (dir != null)
         {
            file = Path.Combine(dir, org);
            if (File.Exists(file)) goto EXIT_RTN;

            dir = Path.GetDirectoryName(dir);
            file = Path.Combine(dir, org);
            if (File.Exists(file)) goto EXIT_RTN;
         }

         //Nothing helps: return original
         file = org;

      EXIT_RTN:
         return Path.GetFullPath(file);
      }

      public ITemplateFactory TemplateFactory { get; set; }
      private XmlHelper loadXml(String fileName, _ImportFlags flags = _ImportFlags.UseFlagsFromXml)
      {
         XmlHelper tmp = new XmlHelper(fileName);
         if (TemplateFactory == null)
         {
            String factoryClass = tmp.ReadStr("@templatefactory", typeof(TemplateFactory).FullName);
            TemplateFactory = CreateObject<ITemplateFactory>(factoryClass, this, tmp);
         }
         _ImportFlags flagsFromXml = tmp.ReadEnum("@importflags", (_ImportFlags)0);

         if (((ImportFlags | flagsFromXml) & _ImportFlags.DebugTemplate) != 0) 
            TemplateFactory.AutoWriteGenerated = true; 

         ImportLog.Log("Flags before load={0}", ImportFlags);

         XmlHelper xml = new XmlHelper();
         ITemplateEngine eng = TemplateFactory.CreateEngine();
         eng.Variables.Set("IMPORT_ROOT", Path.GetDirectoryName(tmp.FileName));
         tmp = null;

         eng.LoadFromFile(fileName);
         MainVariables = eng.Variables;
         FileVariables = eng.FileVariables;
         TemplateFactory.InitialVariables = FileVariables;

         xml.Load (eng.ResultAsReader(), fileName);

         return xml;
      }

      public IVariables MainVariables {get; set;}
      public IVariables FileVariables { get; set; }
      public String TempDir { get; private set; }

      public void Load(XmlHelper xml)
      {
         Xml = xml;
         String dir = xml.FileName;
         if (!String.IsNullOrEmpty(dir)) dir = Path.GetDirectoryName(xml.FileName);
         Environment.SetEnvironmentVariable("IMPORT_ROOT", dir);
         fillTikaVars();

         _ImportFlags flags = ImportFlags;
         ImportFlags = xml.ReadEnum("@importflags", (_ImportFlags)0);
         if ((flags & _ImportFlags.UseFlagsFromXml) == 0)
            ImportFlags = flags;

         LogAdds = xml.ReadInt("@logadds", LogAdds);
         MaxAdds = xml.ReadInt("@maxadds", MaxAdds);
         ImportLog.Log("Loading import xml: flags={0}, logadds={1}, maxadds={2}, file={3}.", ImportFlags, LogAdds, MaxAdds, xml.FileName);

         binDir = Xml.CombinePath(Xml.ReadStr ("@bindir", "bin"));
         if (Directory.Exists(binDir))
            ImportLog.Log(_LogType.ltInfo, "Using extra bin dir: {0}", binDir);
         else
         {
            binDir = null;
            ImportLog.Log(_LogType.ltInfo, "No bin dir found... All executables are loaded from {0}.\r\nCheck bin dir={1}.", Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), binDir);
         }

         TempDir = IOUtils.AddSlash(Xml.CombinePath("temp"));
         IOUtils.ForceDirectories(TempDir, true);

         XmlNode x = Xml.SelectSingleNode("report");
         if (x == null)
            Reporter = null;
         else
            Reporter = new MailReporter(x);
         ImportLog.Log("loaded reporter: {0}", Reporter); 


         //Load the supplied script
         ImportLog.Log(_LogType.ltTimerStart, "loading: scripts"); 
         XmlNode scriptNode = xml.SelectSingleNode("script");

         //Create the holder for the expressions
         ScriptExpressions = new ScriptExpressionHolder();

         ImportLog.Log(_LogType.ltTimer, "loading: helper process definitions ");
         ProcessHostCollection = new ProcessHostCollection(this, xml.SelectSingleNode("processes"));

         ImportLog.Log(_LogType.ltTimer, "loading: endpoints");
         Endpoints = new Endpoints(this, xml.SelectMandatoryNode("endpoints"));

         ImportLog.Log(_LogType.ltTimer, "loading: post-processors");
         PostProcessors = new PostProcessors(this, xml.SelectSingleNode("postprocessors"));

         ImportLog.Log(_LogType.ltTimer, "loading: converters");
         Converters = new Converters(
            xml.SelectSingleNode("converters"),
            "converter",
            (node) => Converter.Create (node),
            false);

         ImportLog.Log(_LogType.ltTimer, "loading: categories");
         Categories = new NamedAdminCollection<CategoryCollection>(
            xml.SelectSingleNode("categories"),
            "collection",
            (node) => new CategoryCollection(node),
            true);

         ImportLog.Log(_LogType.ltTimer, "loading: pipelines");
         Pipelines = new NamedAdminCollection<Pipeline>(
            xml.SelectMandatoryNode("pipelines"),
            "pipeline",
            (node) => new Pipeline(this, node),
            true);

         ImportLog.Log(_LogType.ltTimer, "loading: datasources");
         PipelineContext ctx = new PipelineContext(this);
         Datasources = new NamedAdminCollection<DatasourceAdmin>(
            xml.SelectMandatoryNode("datasources"),
            "datasource",
            (node) => new DatasourceAdmin(ctx, node),
            true);

         //Compile script if needed
         if (ScriptExpressions.Count > 0 || scriptNode != null)
         {
            ScriptHost = new ScriptHost(ScriptHostFlags.Default, TemplateFactory);
            if (scriptNode != null)
            {
               String fn = scriptNode.ReadStr("@file", null);
               if (fn != null)
                  ScriptHost.AddFile(xml.CombinePath(fn));
            }
            ScriptHost.ExtraSearchPath = binDir;
            if (ScriptExpressions.Count > 0)
            {
               String fn = TempDir + "_ScriptExpressions.cs";
               ScriptExpressions.SaveAndClose(fn);
               ScriptHost.AddFile(fn);
            }
            ScriptHost.AddReference(Assembly.GetExecutingAssembly());
            ScriptHost.AddReference(typeof(Bitmanager.Json.JsonExt));
            ScriptHost.AddReference(typeof(Bitmanager.Elastic.ESConnection));
            ScriptHost.Compile();
         }

         ImportLog.Log(_LogType.ltTimerStop, "loading: finished. Loaded {0} datasources, {1} pipelines, {2} endpoints, {3} converters, {4} category collections.",
            Datasources.Count,
            Pipelines.Count,
            Endpoints.Count,
            Converters.Count,
            Categories.Count);
      }

      private void fillTikaVars()
      {
         String dir = IOUtils.FindDirectoryToRoot(Assembly.GetExecutingAssembly().Location, @"TikaService", FindToTootFlags.ReturnNull);
         if (String.IsNullOrEmpty(dir)) return;
         Environment.SetEnvironmentVariable("IMPORT_TIKA_SERVICE_DIR", dir);

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
            if (compareWithVersion (f, max) <= 0) continue;
            max = f;
         }
         return max;
      }

      private int compareWithVersion (String f1, String f2)
      {
         if (f1==null || f2==null) return String.Compare(f1, f2, true);

         String[] parts1 = f1.Split('.');
         String[] parts2 = f2.Split('.');

         int N = Math.Min (parts1.Length, parts2.Length);
         for (int i=0; i<N; i++)
         {
            int v1, v2;
            if (i != N-1 && int.TryParse (parts1[i], out v1) && int.TryParse (parts2[i], out v2))
            {
               if (v1 < v2) return -1;
               if (v1 > v2) return 1;
               continue;
            }
            int rc = String.Compare(parts1[i], parts2[i], true);
            if (rc != 0) return rc;
         }

         if (parts1.Length > N) return 1;
         if (parts2.Length > N) return -1;
         return 0;
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
         ImportLog.Log(_LogType.ltProgress, "Starting import. VirtMem={3:F1}GB, Flags={0}, MaxAdds={1}, ActiveDS's='{2}'.", ImportFlags, MaxAdds, enabledDSses == null ? null : String.Join(", ", enabledDSses), OS.GetTotalVirtualMemory() / (1024 * 1024 * 1024.0));

         PipelineContext mainCtx = new PipelineContext(this);
         try
         {
            _ErrorState stateFilter = _ErrorState.All;
            if ((ImportFlags & _ImportFlags.IgnoreLimited) != 0) stateFilter &= ~_ErrorState.Limited;
            if ((ImportFlags & _ImportFlags.IgnoreErrors) != 0) stateFilter &= ~_ErrorState.Error;

            Endpoints.Open(mainCtx);

            for (int i = 0; i < Datasources.Count; i++)
            {
               DatasourceAdmin admin = Datasources[i];
               if (!String.IsNullOrEmpty(OverrideEndpoint))
               {
                  ImportLog.Log(_LogType.ltWarning, "Datsource {0} will run with the override endpoint={1}", admin.Name, OverrideEndpoint);
                  admin.EndpointName = OverrideEndpoint;
               }
               if (!String.IsNullOrEmpty(OverrideEndpoint))
               {
                  ImportLog.Log(_LogType.ltWarning, "Datsource {0} will run with the override pipeline={1}", admin.Name, OverridePipeline);
                  //admin.p = OverrideEndpoint;
               }

               if (!isActive(enabledDSses, admin))
               {
                  ImportLog.Log(_LogType.ltProgress, "[{0}]: not active", admin.Name);
                  continue;
               }

               var report = new DatasourceReport(admin);
               ret.Add(report);
               PipelineContext ctx = new PipelineContext(this, admin, report);
               var pipeline = admin.Pipeline;

               try
               {
                  admin.Import(ctx);
                  mainCtx.ErrorState |= (ctx.ErrorState & stateFilter);
                  if (ctx.LastError != null) mainCtx.LastError = ctx.LastError;
                  report.MarkEnded (ctx);
               }
               catch (Exception err)
               {
                  mainCtx.LastError = err;
                  mainCtx.ErrorState |= (ctx.ErrorState & stateFilter) | _ErrorState.Error;
                  report.MarkEnded(ctx);
                  throw;
               }
               Endpoints.OptClosePerDatasource(ctx);

               foreach (var c in Converters) c.DumpMissed(ctx);
            }
            ImportLog.Log(_LogType.ltProgress, "Import ended");
            ProcessHostCollection.StopAll();
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
            }
            catch (Exception e2)
            {
               ErrorLog.Log(e2);
               ImportLog.Log(e2);
            }
            try
            {
               ProcessHostCollection.StopAll();
            }
            catch (Exception e2)
            {
               ErrorLog.Log(e2);
               ImportLog.Log(e2);
            }
            try
            {
               ret.SetGlobalStatus(mainCtx);
               if (Reporter != null) Reporter.SendReport(mainCtx, ret);
            }
            catch (Exception e2)
            {
               ErrorLog.Log(e2);
               ImportLog.Log(e2);
            }
         }
         //ret.SetGlobalStatus(mainCtx);
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
               case "excel": return typeof(ExcelDatasource).FullName;
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

      public static String ReadScriptNameOrBody (XmlNode x, String attr, out String body, bool mandatory=false)
      {
         String name = x.ReadStr(attr, null);
         body = x.ReadStr(null, null).TrimWhiteSpaceToNull();
         if (body!=null)
         {
            if (name != null)
               throw new BMNodeException (x, "Attribute {0} is not allowed when the node has a script body.", attr.Substring(1));
         } else
         {
            if (name==null && mandatory)
               throw new BMNodeException(x, "A script needs to be specified via the {0} attribute or as a body on the node.", attr.Substring(1));
         }
         return name;

      }
   }
}
