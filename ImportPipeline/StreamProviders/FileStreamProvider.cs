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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class FileStreamProvider : StreamProviderBase
   {
      public FileStreamProvider(PipelineContext ctx, FileCollectionStreamProvider parent, FileCollectionStreamProvider._FileElt fileElt)
         : base(parent, parent.contextNode)
      {
         credentialsInitialized = true;
         base.SetNames(fileElt.Name, parent.RootLen, parent.VirtualRoot);
         base.SetMeta(fileElt.LastWriteUtc, fileElt.Size);
         uri = new Uri ("file://" + fullName);
      }

      public override Stream CreateStream()
      {
         return new FileStream(fullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 16*1024);
      }
   }

   public class FileCollectionStreamProvider : StreamProviderBaseBase, IStreamProviderCollection
   {
      public enum SortMode { None = 0, FileName = 1, FileSize = 2, FileDate = 4, Asc = 8, Desc = 16 };
      public SortMode Sort { get; set; }
      public bool IgnoreDates { get; set; }
      private bool recursive;

      private static Logger errLogger = Logs.ErrorLog.Clone("FileStreamProvider");

      public readonly String Root, File;
      public readonly String VirtualRoot;
      public readonly int RootLen;
      private FileTree tree;
      private PipelineContext ctx;

      public FileCollectionStreamProvider(PipelineContext ctx, XmlNode node, XmlNode parentNode)
         : base(node)
      {
         this.ctx = ctx;
         VirtualRoot = XmlUtils.ReadStr(node, "@virtualroot", null);
         Sort = node.ReadEnum("@filesort", SortMode.FileName | SortMode.Desc);
         IgnoreDates = node.ReadBool("@ignoredates", false);
         if ((ctx.ImportFlags & _ImportFlags.FullImport) != 0)
            IgnoreDates = true;

         String file = node.ReadStr("@file", null);
         String root = node.ReadStr("@root", null);
         if (file == null && root == null) throw new BMNodeException(node, "Missing file/root attribute.");

         if (root != null && file != null)
         {
            file = Path.Combine(root, file);
            root = null;
         }

         if (file != null)
         {
            File = ctx.ImportEngine.Xml.CombinePath(file);
            RootLen = 1 + Path.GetDirectoryName(File).Length;
         }
         else
         {
            tree = new FileTree();
            Root = root = Path.GetFullPath (ctx.ImportEngine.Xml.CombinePath(root));
            RootLen = root.Length;
            if (root[root.Length - 1] != '\\') RootLen++;

            recursive = XmlUtils.ReadBool(node, "@recursive", true);
            String filter = XmlUtils.ReadStr(node, "@filter", null);

            tree.OnFile += tree_OnFile;
            tree.OnFileError += fileTree_OnFileError;

            if (filter != null) tree.AddFileFilter(filter, true);

            addFilters(tree, node.SelectNodes("dir"), true);
            addFilters(tree, node.SelectNodes("file"), false);
         }

      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.Append(GetType().Name);
         sb.Append(" [");
         bool needComma = false;
         if (Root != null)
         {
            needComma = true;
            sb.Append("root=");
            sb.Append(Root);
         }
         if (File != null)
         {
            if (needComma) sb.Append(", ");
            sb.Append("file=");
            sb.Append(File);
         }
         sb.Append(']');
         return sb.ToString();
      }

      public IEnumerable<IStreamProviderBase> GetElements(PipelineContext ctx)
      {
         this.ctx = ctx; //Save for future use in places where we don't have a context
         foreach (_FileElt elt in getFiles(ctx))
            yield return new FileStreamProvider(ctx, this, elt);
      }

      private DateTime getMinDate()
      {
         if (IgnoreDates || ctx == null || (ctx.ImportFlags & _ImportFlags.FullImport) != 0 || ctx.RunAdministrations==null) return DateTime.MinValue;
         DateTime ret = ctx.RunAdministrations.GetLastOKRunDateShifted(ctx.DatasourceAdmin);
         ctx.ImportLog.Log("Enumerating files using minDate (utc): {0}", ret.ToUniversalTime());
         return ret;
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

      List<_FileElt> getFiles (PipelineContext ctx)
      {
         var list = new List<_FileElt>();
         if (tree != null)
         {
            tree.MinUtcDate = getMinDate();
            tree.UserTag = list;  //We fill list instead of tree.Files...
            tree.ReadFiles(Root, recursive ? _ReadFileFlags.rfSubdirs : 0);
            if (ctx != null) ctx.ImportLog.Log("-- Found {0} files.", list.Count);
            goto EXIT_RTN;
         }

         if (File.IndexOf('*') < 0 && File.IndexOf('?') < 0)
         {
            list.Add(new _FileElt(File));
            goto EXIT_RTN;
         }

         String dir = Path.GetDirectoryName(File);

         var dirInfo = new DirectoryInfo(dir);
         var files = dirInfo.GetFileSystemInfos(Path.GetFileName(File));

         DateTime minUtc = getMinDate();
         DateTime maxUtc = DateTime.MaxValue;
         foreach (var info in files)
         {
            if (info.LastWriteTimeUtc < minUtc) continue;
            if (info.LastWriteTimeUtc >= maxUtc) continue;
            list.Add(new _FileElt(info));
         }
 
         EXIT_RTN:
         //Sort the list if requested and return the sorted list
         if (list.Count > 1)
         {
            Comparison<_FileElt> cmp = GetSorter();
            if (cmp != null) list.Sort(cmp);
         }
         return list;
      }

      void tree_OnFile(FileTree sender, string RelativeFName, FileSystemInfo info, object userTag)
      {
         var list = (List<_FileElt>)userTag;
         list.Add(new _FileElt(info));
      }

      void fileTree_OnFileError(FileTree sender, FileTree.ErrorArguments args)
      {
         errLogger.Log("Cannot read '{0}': {1}.", args.FileName, args.Error);
         errLogger.Log(args.Error);
         args.Continue = true;
      }



      public class _FileElt
      {
         public readonly String Name;
         public readonly DateTime LastWriteUtc;
         public readonly long Size;

         public _FileElt(FileSystemInfo fi)
         {
            Name = fi.FullName;
            LastWriteUtc = fi.LastWriteTimeUtc;
            Size = -1;
         }
         public _FileElt(String fullName)
         {
            Name = fullName;
            LastWriteUtc = System.IO.File.GetLastWriteTimeUtc(fullName);
            Size = -1;
         }

         public static int sortAscName(_FileElt left, _FileElt right)
         {
            return String.Compare(left.Name, right.Name, StringComparison.InvariantCultureIgnoreCase);
         }
         public static int sortDescName(_FileElt left, _FileElt right)
         {
            return String.Compare(right.Name, left.Name, StringComparison.InvariantCultureIgnoreCase);
         }
         public static int sortAscDate(_FileElt left, _FileElt right)
         {
            return DateTime.Compare(left.LastWriteUtc, right.LastWriteUtc);
         }
         public static int sortDescDate(_FileElt left, _FileElt right)
         {
            return DateTime.Compare(right.LastWriteUtc, left.LastWriteUtc);
         }
      }


      Comparison<_FileElt> GetSorter()
      {
         switch (Sort & (SortMode.FileName | SortMode.FileDate | SortMode.FileSize))
         {
            default: return null;
            case SortMode.FileDate:
               return (Sort & SortMode.Desc) != 0 ? (Comparison<_FileElt>)_FileElt.sortDescDate : (Comparison<_FileElt>)_FileElt.sortAscDate;
            case SortMode.FileName:
               return (Sort & SortMode.Desc) != 0 ? (Comparison<_FileElt>)_FileElt.sortDescName : (Comparison<_FileElt>)_FileElt.sortAscName;
         }
      }


   }
}
