using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.IO;
using System.IO;

namespace Bitmanager.ImportPipeline
{
   public class FileNameFeeder : IDatasourceFeeder, IEnumerable<IDatasourceFeederElement>
   {
      private String file;
      private XmlNode ctxNode;
      private FileTree tree;
      private string root, virtualRoot;
      private bool recursive;
      public  bool IgnoreDates {get; set;}
      private static Logger errLogger = Logs.ErrorLog.Clone("FileNameFeeder");

      private static FeederElementBase createUri(XmlNode ctx, Uri baseUri, String url)
      {
         return new FeederElementBase(ctx, baseUri == null ? new Uri(url) : new Uri(baseUri, url));
      }
      public void Init(PipelineContext ctx, XmlNode node)
      {
         IgnoreDates = node.ReadBool("@ignoredates", false);
         if ((ctx.ImportFlags & _ImportFlags.FullImport) != 0)
            IgnoreDates = true;

         file = node.ReadStr("@file", null);
         this.ctxNode = node;
         if (file != null)
            file = ctx.ImportEngine.Xml.CombinePath(file);
         else
         {
            tree = new FileTree();
            root = XmlUtils.ReadStr(node, "@root");
            virtualRoot = XmlUtils.ReadStr(node, "@virtualroot", null);
            recursive = XmlUtils.ReadBool(node, "@recursive", true);

            String filter = XmlUtils.ReadStr(node, "@filter", null);

            tree.OnFileError += fileTree_OnFileError;
            if (filter != null) tree.AddFileFilter(filter, true);

            addFilters(tree, node.SelectNodes("dir"), true);
            addFilters(tree, node.SelectNodes("file"), false);
         }
      }

      private static void addFilters(FileTree tree, XmlNodeList nodes, bool isDir)
      {
         foreach (XmlNode node in nodes)
         {
            String incl = XmlUtils.ReadStr(node, "@incl", null);
            String excl = XmlUtils.ReadStr(node, "@excl", null);
            if (incl == null && excl == null)
               throw new BMNodeException(node, "At least 1 of the attributes incl or excl must be present.");

            if (incl != null)
            {
               if (isDir) tree.AddDirFilter(incl, true); else tree.AddFileFilter(incl, true);
            }

            if (excl != null)
            {
               if (isDir) tree.AddDirFilter(excl, false); else tree.AddFileFilter(excl, false);
            }
         }
      }

      void fileTree_OnFileError(FileTree sender, FileTree.ErrorArguments args)
      {
         errLogger.Log("Cannot read '{0}': {1}.", args.FileName, args.Error);
         errLogger.Log(args.Error);
         args.Continue = true;
      }


      private List<String> getFilesFromFileSpec2 (String file, DateTime minUtc, DateTime maxUtc)
      {
         var arr = new List<string>();
         if (file.IndexOf('*') < 0 && file.IndexOf('?') < 0)
         {
            arr.Add (file);
            goto EXIT_RTN;
         }
         String dir = Path.GetDirectoryName(file);
         if (!Directory.Exists(dir)) goto EXIT_RTN;

         var dirInfo = new DirectoryInfo(dir);
         var files = dirInfo.GetFileSystemInfos (Path.GetFileName(file));
         foreach (var info in files) {
            if (info.LastWriteTimeUtc < minUtc) continue;
            if (info.LastWriteTimeUtc >= maxUtc) continue;
            arr.Add (info.FullName);
         }
         EXIT_RTN:
         return arr;
      }

      //private static String[] getFilesFromFileSpec(String file)
      //{
      //   if (file.IndexOf('*') < 0 && file.IndexOf('?') < 0)
      //   {
      //      String[] arr = new String[1];
      //      arr[0] = file;
      //      return arr;
      //   }
      //   String dir = Path.GetDirectoryName(file);
      //   String spec = Path.GetFileName(file);
      //   if (!Directory.Exists(dir)) return new String[0];
      //   return Directory.GetFiles(dir, spec);
      //}


      //public static class WildcardMatch
      //{
      //   #region Public Methods
      //   public static bool IsLike(string pattern, string text, bool caseSensitive = false)
      //   {
      //      pattern = pattern.Replace(".", @"\.");
      //      pattern = pattern.Replace("?", ".");
      //      pattern = pattern.Replace("*", ".*?");
      //      pattern = pattern.Replace(@"\", @"\\");
      //      pattern = pattern.Replace(" ", @"\s");
      //      return new Regex(pattern, caseSensitive ? RegexOptions.None : RegexOptions.IgnoreCase).IsMatch(text);
      //   }
      //   #endregion
      //}

      private DateTime getMinDate ()
      {
         if (IgnoreDates || _ctx==null || (_ctx.ImportFlags & _ImportFlags.FullImport) != 0) return DateTime.MinValue;
         DateTime ret = _ctx.RunAdministrations.GetLastOKRunDate(_ctx.DatasourceAdmin.Name);
         _ctx.ImportLog.Log ("Enumerating files using minDate (utc): {0}", ret.ToUniversalTime());
         return ret;
      }
      private PipelineContext _ctx;
      public IEnumerable<IDatasourceFeederElement> GetElements (PipelineContext ctx)
      {
         _ctx = ctx;
         return this;
      }

      public IEnumerator<IDatasourceFeederElement> GetEnumerator()
      {
         if (file != null)
         {
            foreach (var s in getFilesFromFileSpec2(file, getMinDate(), DateTime.MaxValue))
            {
               //Logs.ErrorLog.Log("File=" + s);
               yield return new FileNameFeederElement(ctxNode, s);
            }
         }
         else
         {
            tree.MinUtcDate = getMinDate ();
            tree.ReadFiles(root, recursive ? (_ReadFileFlags.rfStoreFiles | _ReadFileFlags.rfSubdirs) : (_ReadFileFlags.rfStoreFiles));
            if (_ctx != null) _ctx.ImportLog.Log("-- Found {0} files.", tree.Files.Count);

            for (int i = 0; i < tree.Files.Count; i++)
               yield return new FileNameFeederElement(ctxNode, tree, tree.Files[i], virtualRoot);
         }
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }
   }

   public class FileNameFeederElement : FeederElementBase
   {
      public readonly String FileName;
      public readonly String RelativeName;
      public readonly String VirtualRoot;
      public readonly String VirtualFileName;

      public FileNameFeederElement(XmlNode ctx, FileTree tree, String relname, String virtualRoot)
         : base(ctx, tree.GetFullName(relname))
      {
         RelativeName = relname;
         FileName = (String)Element;
         VirtualRoot = virtualRoot;
         if (virtualRoot == null)
            VirtualFileName = FileName;
         else
         {
            VirtualRoot = virtualRoot;
            VirtualFileName = Path.Combine(virtualRoot, relname);
         }
      }

      public FileNameFeederElement(XmlNode ctx, String name)
         : base(ctx, name)
      {
         RelativeName = Path.GetFullPath(name);
         FileName = RelativeName;
         VirtualRoot = RelativeName;
      }

   }
}
