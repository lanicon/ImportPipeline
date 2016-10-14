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
using System.Reflection;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   /// <summary>
   /// Provides a filestream with meta-information like a virtual root, relative name, and file-info.
   /// The virtual root might be dynamically calculated, depending on the settings in the FileCollectionStreamProvider.
   /// </summary>
   public class FileStreamProvider : StreamProvider
   {
      public FileStreamProvider(PipelineContext ctx, FileStreamDirectory parent, FileStreamDirectory._FileElt fileElt)
         : base(parent, parent.ContextNode)
      {
         credentialsInitialized = true;
         int rootLen = parent.RootLen;
         String virtualRoot = parent.VirtualRoot;
         if (parent.VirtualRootFromFile)
         {
            int ix = rootLen;
            for (; ix < fileElt.Name.Length; ix++)
            {
               switch (fileElt.Name[ix])
               {
                  default: continue;
                  case '\\':
                  case '/': break;
               }
               if (ix>rootLen) virtualRoot = fileElt.Name.Substring(rootLen, ix - rootLen);
               rootLen = ix + 1;
               break;
            }
         }
         base.SetNames(fileElt.Name, rootLen, virtualRoot);
         base.SetMeta(fileElt.LastWriteUtc, fileElt.Size);
         base.isDir = fileElt.IsDir;
         base.attributes = fileElt.Attributes;
         uri = new Uri ("file://" + fullName);
      }

      public override Stream CreateStream()
      {
         return new FileStream(fullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 16*1024);
      }
   }

}
