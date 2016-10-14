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
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Windows.Forms;
using Bitmanager.Importer;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public enum StreamProtocol {File, Http, Other};

   public interface IStreamProvider
   {
      StreamDirectory Parent { get; }
      XmlNode ContextNode { get; }
      String VirtualName { get; }
      String VirtualRoot { get; }
      String RelativeName { get; }
      String FullName { get; }
      Uri Uri { get; }
      Stream CreateStream();
      CredentialCache Credentials { get; }
      DateTime LastModified { get; }
      FileAttributes Attributes { get; }
      long Size { get; }
      bool IsDir { get; }
   }

   /// <summary>
   /// Base class for interface IStreamProvider
   /// </summary>
   public abstract class StreamProvider : IStreamProvider
   {
      protected StreamDirectory parent;
      protected XmlNode contextNode;
      protected String fullName, virtualName, virtualRoot, relativeName;
      protected Uri uri;
      protected CredentialCache credentialCache;
      protected StreamProtocol protocol;
      protected DateTime lastModUtc;
      protected FileAttributes attributes;
      protected long size;
      protected bool credentialsInitialized;
      protected bool credentialsNeeded;
      protected bool silent;
      protected bool isDir;


      public StreamProvider(XmlNode contextNode)
      {
         this.contextNode = contextNode;
         lastModUtc = DateTime.MinValue;
         size = -1;
      }
      public StreamProvider(StreamDirectory parent, XmlNode contextNode)
      {
         this.contextNode = contextNode;
         this.parent = parent;
         lastModUtc = DateTime.MinValue;
         size = -1;
      }


      protected static String ToForwardSlash(String x)
      {
         return x == null ? null : x.Replace('\\', '/');
      }
      protected static String ToBackwardSlash(String x)
      {
         return x == null ? null : x.Replace('/', '\\');
      }
      protected void NamesToForwardSlash()
      {
         fullName = ToForwardSlash(fullName);
         relativeName = ToForwardSlash(relativeName);
         virtualName = ToForwardSlash(virtualName);
         virtualRoot = ToForwardSlash(virtualRoot);
      }
      protected void NamesToBackwardSlash()
      {
         fullName = ToBackwardSlash(fullName);
         relativeName = ToBackwardSlash(relativeName);
         virtualName = ToBackwardSlash(virtualName);
         virtualRoot = ToBackwardSlash(virtualRoot);
      }
      protected void SetNames(String fullName, int rootLen, String virtualRoot)
      {
         SetNames(fullName, fullName.Substring(rootLen), virtualRoot);
      }

      protected void SetNames(String fullName, String relName, String virtualRoot)
      {
         this.fullName = fullName;
         this.relativeName = relName;
         if (virtualRoot == null)
            this.virtualName = fullName;
         else
         {
            this.virtualName = Path.Combine(virtualRoot, relName);
            this.virtualRoot = virtualRoot;
         }
      }

      protected void SetMeta(DateTime lastMod, long size)
      {
         this.lastModUtc = lastMod;
         this.size = size;
      }

      public virtual string FullName
      {
         get { return fullName; }
      }

      public virtual Uri Uri
      {
         get { return uri; }
      }

      protected virtual bool promptForPassword(ref String user, out String password)
      {
         return CredentialsHelper.PromptForCredentials(null, Uri.ToString(), ref user, out password);
      }

      protected virtual CredentialCache InitCredentials()
      {
         String user = contextNode.ReadStr("@user", null);
         if (user == null)
         {
            if (credentialsNeeded) throw new BMNodeException(contextNode, "Missing username/password.");
            return null;
         }
         String password = contextNode.ReadStr("@password", null);
         if (password == "?")
         {
            if (silent) throw new BMNodeException(contextNode, "Cannot prompt for password due to silent mode.");
            if (!promptForPassword(ref user, out password))
               throw new BMNodeException(contextNode, "Prompt for password cancelled.");
         }

         CredentialCache credsCache = new CredentialCache();
         NetworkCredential myCred = new NetworkCredential(user, password);
         credsCache.Add(uri, "Basic", myCred);
         return credsCache;
      }

      public override string ToString()
      {
         return fullName;
      }

      public CredentialCache Credentials
      {
         get
         {
            if (credentialsInitialized) return this.credentialCache;

            credentialsInitialized = true;
            return credentialCache = InitCredentials();
         }
      }


      public abstract Stream CreateStream();


      public string VirtualName
      {
         get { return virtualName; }
      }

      public string VirtualRoot
      {
         get { return virtualRoot; }
      }

      public string RelativeName
      {
         get { return relativeName; }
      }

      public DateTime LastModified
      {
         get { return lastModUtc; }
      }

      public FileAttributes Attributes
      {
         get { return attributes; }
      }

      public long Size
      {
         get { return size; }
      }

      public bool IsDir
      {
         get { return isDir; }
      }

      public StreamDirectory Parent
      {
         get { return parent; }
      }

      public XmlNode ContextNode
      {
         get { return contextNode; }
      }
   }
}
