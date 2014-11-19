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
   public class FileNameFeeder : IDatasourceFeeder
   {
      private String file;
      private XmlNode ctxNode;
      private FileTree tree;
      private string root, virtualRoot;
      private bool recursive;
      private static Logger errLogger = Logs.ErrorLog.Clone("FileNameFeeder");

      private static FeederElementBase createUri(XmlNode ctx, Uri baseUri, String url)
      {
         return new FeederElementBase(ctx, baseUri == null ? new Uri(url) : new Uri(baseUri, url));
      }
      public void Init(PipelineContext ctx, XmlNode node)
      {
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

      private static String[] getFilesFromFileSpec(String file)
      {
         if (file.IndexOf('*') < 0 && file.IndexOf('?') < 0)
         {
            String[] arr = new String[1];
            arr[0] = file;
            return arr;
         }
         String dir = Path.GetDirectoryName(file);
         String spec = Path.GetFileName(file);
         if (!Directory.Exists(dir)) return new String[0];
         return Directory.GetFiles(dir, spec);
      }
      public IEnumerator<IDatasourceFeederElement> GetEnumerator()
      {
         if (file != null)
         {
            foreach (var s in getFilesFromFileSpec(file))
            {
               Logs.ErrorLog.Log("File=" + s);
               yield return new FileNameFeederElement(ctxNode, s);
            }
         }
         else
         {
            tree.ReadFiles(root, recursive ? (_ReadFileFlags.rfStoreFiles | _ReadFileFlags.rfSubdirs) : (_ReadFileFlags.rfStoreFiles));

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
