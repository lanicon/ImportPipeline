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

   /// <summary>
   /// Base class for stream providers, that stores common properties and handles credentials
   /// </summary>
   public abstract class StreamProviderBase
   {
      protected XmlNode contextNode;
      protected CredentialCache credentialCache;
      protected NetworkCredential credential;
      protected StreamProtocol protocol;
      protected String url;
      protected bool credentialsNeeded, credentialsInitialized, silent;

      protected StreamProviderBase(XmlNode contextNode, String url)
      {
         this.contextNode = contextNode;
         this.url = url;
      }
      protected StreamProviderBase(StreamProviderBase other)
      {
         contextNode = other.contextNode;
         credentialCache = other.credentialCache;
         credential = other.credential;
         protocol = other.protocol;
         credentialsNeeded = other.credentialsNeeded;
         credentialsInitialized = other.credentialsInitialized;
         silent = other.silent;
      }

      protected virtual bool promptForPassword(ref String user, out String password)
      {
         return CredentialsHelper.PromptForCredentials(null, url, ref user, out password);
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
         credential = new NetworkCredential(user, password);
         if (url != null)
            credsCache.Add(new Uri(url), "Basic", credential);
         return credsCache;
      }

      protected CredentialCache GetCredentials()
      {
         if (credentialsInitialized) return this.credentialCache;

         credentialsInitialized = true;
         return credentialCache = InitCredentials();
      }
      protected NetworkCredential GetCredential()
      {
         if (credentialsInitialized) return this.credential;

         credentialsInitialized = true;
         InitCredentials();
         return this.credential;
      }
   }
}
