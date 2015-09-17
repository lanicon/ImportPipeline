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
      public enum SortMode { None=0, FileName = 1, FileSize = 2, FileDate = 4, Asc = 8, Desc = 16 };
      private String file;
      private XmlNode ctxNode;
      private FileTree tree;
      private string root, virtualRoot;
      private int rootLen;
      private bool recursive;
      public  bool IgnoreDates {get; set;}
      public SortMode Sort{get; set;}

      private static Logger errLogger = Logs.ErrorLog.Clone("FileNameFeeder");

      private static FeederElementBase createUri(XmlNode ctx, Uri baseUri, String url)
      {
         return new FeederElementBase(ctx, baseUri == null ? new Uri(url) : new Uri(baseUri, url));
      }

      public void Init(PipelineContext ctx, XmlNode node)
      {
         Sort = node.ReadEnum("@filesort", SortMode.FileName | SortMode.Desc);
         IgnoreDates = node.ReadBool("@ignoredates", false);
         if ((ctx.ImportFlags & _ImportFlags.FullImport) != 0)
            IgnoreDates = true;

         file = node.ReadStr("@file", null);
         this.ctxNode = node;
         if (file != null)
         {
            file = ctx.ImportEngine.Xml.CombinePath(file);
            rootLen = 1 + Path.GetDirectoryName(file).Length;
         }
         else
         {
            tree = new FileTree();
            root = XmlUtils.ReadStr(node, "@root");
            root = ctx.ImportEngine.Xml.CombinePath(root);
            rootLen = 1 + root.Length;
            if (root[root.Length - 1] != '\\') rootLen--;

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

      class _FileElt
      {
         public readonly String Name;
         public readonly DateTime LastWriteUtc;
         public readonly long Size;

         public _FileElt(FileSystemInfo fi)
         {
            Name = fi.FullName;
            LastWriteUtc = fi.LastWriteTimeUtc;
         }
         public _FileElt(String fullName)
         {
            Name = fullName;
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


      Comparison<_FileElt> GetSorter ()
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

      List<_FileElt> SortElts (List<_FileElt> elts)
      {
         if (elts == null || elts.Count <= 1) return elts;
         Comparison<_FileElt> cmp = GetSorter();
         if (cmp != null)
            elts.Sort (cmp);
         return elts;
      }
      private List<_FileElt> getFilesFromFileSpec2(String file, DateTime minUtc, DateTime maxUtc)
      {
         var arr = new List<_FileElt>();
         if (file.IndexOf('*') < 0 && file.IndexOf('?') < 0)
         {
            arr.Add (new _FileElt(file));
            goto EXIT_RTN;
         }
         String dir = Path.GetDirectoryName(file);

         var dirInfo = new DirectoryInfo(dir);
         var files = dirInfo.GetFileSystemInfos (Path.GetFileName(file));
         foreach (var info in files) {
            if (info.LastWriteTimeUtc < minUtc) continue;
            if (info.LastWriteTimeUtc >= maxUtc) continue;
            arr.Add (new _FileElt (info));
         }
         EXIT_RTN:
         return SortElts(arr);
      }

      private DateTime getMinDate ()
      {
         if (IgnoreDates || _ctx==null || (_ctx.ImportFlags & _ImportFlags.FullImport) != 0) return DateTime.MinValue;
         DateTime ret = _ctx.RunAdministrations.GetLastOKRunDateShifted(_ctx.DatasourceAdmin);
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
            foreach (var s in SortElts(getFilesFromFileSpec2(file, getMinDate(), DateTime.MaxValue)))
            {
               //Logs.ErrorLog.Log("File=" + s);
               yield return new FileNameFeederElement(ctxNode, rootLen, s.Name, virtualRoot);
            }
         }
         else
         {
            var list = new List<_FileElt>();
            tree.MinUtcDate = getMinDate ();
            tree.OnFile += tree_OnFile;
            tree.UserTag = list;
            tree.ReadFiles(root, recursive ? _ReadFileFlags.rfSubdirs : 0);
            if (_ctx != null) _ctx.ImportLog.Log("-- Found {0} files.", tree.Files.Count);

            SortElts(list);
            for (int i = 0; i < list.Count; i++)
               yield return new FileNameFeederElement(ctxNode, rootLen, list[i].Name, virtualRoot);
         }
      }

      void tree_OnFile(FileTree sender, string RelativeFName, FileSystemInfo info, object userTag)
      {
         var list = (List<_FileElt>)userTag;
         list.Add(new _FileElt(info));
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

      public FileNameFeederElement(XmlNode ctx, int rootLen, String fullName, String virtualRoot)
         : base(ctx, fullName)
      {
         RelativeName = fullName.Substring(rootLen);
         FileName = (String)Element;
         VirtualRoot = virtualRoot;
         if (virtualRoot == null)
            VirtualFileName = FileName;
         else
         {
            VirtualRoot = virtualRoot;
            VirtualFileName = Path.Combine(virtualRoot, RelativeName);
         }
      }

   }
}
