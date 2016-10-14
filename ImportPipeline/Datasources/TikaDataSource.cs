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

using Bitmanager.ImportPipeline;
using Bitmanager.Core;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Net;
using System.IO;
using HtmlAgilityPack;
using System.Text.RegularExpressions;
using System.Web;
using System.Threading;
using Bitmanager.Elastic;
using Bitmanager.IO;
using System.Security.Principal;
using System.Security.AccessControl;
using Bitmanager.ImportPipeline.StreamProviders;

namespace Bitmanager.ImportPipeline
{
   public class TikaDS: Datasource
   {
      private String processName;
      public String UriBase {get; private set;}
      public String DbgStoreDir { get; private set; }
      private RootStreamDirectory streamDirectory;
      private String pingUrl;
      private int pingTimeout;
      private int abstractLength, abstractDelta;
      private int maxParallel;
      private bool mustEmitSecurity;

      private AsyncRequestQueue workerQueue;
      private SecurityCache securityCache;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         processName = node.ReadStr("@tikaprocess");
         UriBase = node.ReadStr("@tikaurl");
         if (!UriBase.EndsWith("/")) UriBase += "/";
         streamDirectory = new RootStreamDirectory(ctx, node);
         abstractLength = node.ReadInt("abstract/@maxlength", 256);
         abstractDelta = node.ReadInt("abstract/@delta", 20);
         DbgStoreDir = node.ReadStr("dbgstore", null);
         if (DbgStoreDir != null)
         {
            DbgStoreDir = IOUtils.AddSlash(ctx.ImportEngine.Xml.CombinePath(DbgStoreDir));
            IOUtils.ForceDirectories(DbgStoreDir, true);
         }
         ctx.ImportLog.Log("dbgstore dir={0}", DbgStoreDir ?? "NULL");

         pingUrl = node.ReadStr("@pingurl", null);
         pingTimeout = node.ReadInt("@pingtimeout", 10000);
         maxParallel = node.ReadInt("@maxparallel", 1);
         mustEmitSecurity = node.ReadBool("security/@emit", false); 
      }

      private DateTime previousRun;
      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         workerQueue = AsyncRequestQueue.Create(maxParallel);
         ctx.ImportLog.Log("TikaDS starting. maxparallel={0}, dbgstore={1}, Q={2}", maxParallel, DbgStoreDir, workerQueue);
         if (maxParallel >= 2 && ServicePointManager.DefaultConnectionLimit < maxParallel)
         {
            ctx.ImportLog.Log("Updating connectionLimit for {0} to {1}", ServicePointManager.DefaultConnectionLimit, maxParallel);
            ServicePointManager.DefaultConnectionLimit = maxParallel;
         }

         ensureTikaServiceStarted(ctx);
         previousRun = ctx.RunAdministrations.GetLastOKRunDateShifted(ctx.DatasourceAdmin);
         ctx.ImportLog.Log("Previous (shifted) run was {0}.", previousRun);
         //GenericStreamProvider.DumpRoots(ctx, streamDirectory);
         try
         {
            if (this.mustEmitSecurity) securityCache = new SecurityCache(TikaSecurityAccount.FactoryImpl);
            foreach (var elt in streamDirectory.GetProviders(ctx))
            {
               try
               {
                  importUrl(ctx, sink, elt);
               }
               catch (Exception e)
               {
                  throw new BMException(e, "{0}\r\nUrl={1}.", e.Message, elt);
               }
            }
            //Handle still queued workers
            while (true)
            {
               TikaAsyncWorker popped = pushPop(ctx, sink, null);
               if (popped == null) break;
               importUrl(ctx, sink, popped);
            }
         }
         finally
         {
            workerQueue.PopAllWithoutException();
            Utils.FreeAndNil(ref securityCache);
         }
      }

      private static ExistState toExistState (Object result)
      {
         if (result == null || !(result is ExistState)) return ExistState.NotExist;
         return (ExistState)result;
      }

      private TikaAsyncWorker pushPop(PipelineContext ctx, IDatasourceSink sink, TikaAsyncWorker newElt)
      {
         try
         {
            return (TikaAsyncWorker)((newElt == null) ? workerQueue.Pop() : workerQueue.PushAndOptionalPop(newElt));
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
            return null;
         }
      }
      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         ctx.IncrementEmitted();
         TikaAsyncWorker worker = new TikaAsyncWorker(this, elt);
         String fileName = elt.FullName;
         sink.HandleValue (ctx, "record/_start", fileName);
         sink.HandleValue(ctx, "record/lastmodutc", worker.LastModifiedUtc);
         sink.HandleValue(ctx, "record/virtualFilename", elt.VirtualName);

         //Check if we need to convert this file
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            if ((ctx.ImportFlags & _ImportFlags.RetryErrors)==0 && worker.LastModifiedUtc < previousRun)
            {
               ctx.Skipped++;
               return;
            }
            ExistState existState = toExistState (sink.HandleValue(ctx, "record/_checkexist", elt));
            if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
            {
               ctx.Skipped++;
               return;
            }
         }

         TikaAsyncWorker popped = pushPop(ctx, sink, worker);
         if (popped != null)
            importUrl(ctx, sink, popped);
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, TikaAsyncWorker worker)
      {
         String fileName = worker.StreamElt.FullName;
         sink.HandleValue(ctx, "record/_start", fileName);
         sink.HandleValue(ctx, "record/lastmodutc", worker.LastModifiedUtc);
         sink.HandleValue(ctx, "record/virtualFilename", worker.StreamElt.VirtualName);
         sink.HandleValue(ctx, "record/virtualRoot", worker.StreamElt.VirtualRoot);

         try
         {

            var htmlProcessor = worker.HtmlProcessor;
            if (worker.StoredAs != null) sink.HandleValue(ctx, "record/converted_file", worker.StoredAs);

            //Write html properties
            foreach (var kvp in htmlProcessor.Properties)
            {
               sink.HandleValue(ctx, "record/" + kvp.Key, kvp.Value);
            }

            if (mustEmitSecurity) emitSecurity(ctx, sink, fileName);
            //Add dummy type to recognize the errors
            //if (error)
            //   doc.AddField("content_type", "ConversionError");
            //if (htmlProcessor.IsTextMail)
            sink.HandleValue(ctx, "record/_istextmail", htmlProcessor.IsTextMail);
            sink.HandleValue(ctx, "record/_numparts", htmlProcessor.numParts);
            sink.HandleValue(ctx, "record/_numattachments", htmlProcessor.Attachments.Count);
            foreach (var a in htmlProcessor.Attachments)
                sink.HandleValue(ctx, "record/_attachment", a);
            sink.HandleValue(ctx, "record/_filesize", worker.FileSize);
            sink.HandleValue(ctx, "record/shortcontent", htmlProcessor.GetAbstract(abstractLength, abstractDelta));

            sink.HandleValue(ctx, "record/head", htmlProcessor.GetInnerHead());
            sink.HandleValue(ctx, "record/content", htmlProcessor.GetInnerBody());

            sink.HandleValue(ctx, "record/_end", fileName);
            sink.HandleValue(ctx, "record", null);
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }

      private void emitSecurity(PipelineContext ctx, IDatasourceSink sink, String fileName)
      {
         FileInfo info = new FileInfo(fileName);
         var ac = info.GetAccessControl();
         var rules = ac.GetAccessRules(true, true, typeof(NTAccount));
         foreach (AuthorizationRule rule in rules)
         {
            FileSystemAccessRule fsRule = rule as FileSystemAccessRule;
            if (fsRule.AccessControlType == AccessControlType.Deny) continue;
            //ctx.ImportLog.Log("rule2 {0}: {1}", securityCache.GetAccount(rule.IdentityReference), fsRule.FileSystemRights);
            if ((fsRule.FileSystemRights & FileSystemRights.ReadData) == 0) continue;

            String access = null;
            switch (fsRule.AccessControlType)
            {
               case AccessControlType.Allow: access = "/allow"; break;
               case AccessControlType.Deny: access = "/deny"; break;
               default: access = "/" + fsRule.ToString().ToLowerInvariant(); break;
            }

            var account = securityCache.GetAccount(rule.IdentityReference);
            if (account.WellKnownSid != null)
            {
               WellKnownSidType sidType = (WellKnownSidType)account.WellKnownSid;
               //ctx.ImportLog.Log("wellksid={0}", sidType);
               switch (sidType)
               {
                  case WellKnownSidType.AuthenticatedUserSid:
                  case WellKnownSidType.WorldSid:
                     break;
                  default: continue;
               }
            }
            else
            {
               if (!account.IsGroup) continue;
            }
            sink.HandleValue(ctx, "record/security/group" + access, account);
         }
      }

      private void ensureTikaServiceStarted(PipelineContext ctx)
      {
         ctx.ImportLog.Log(_LogType.ltTimerStart, "tika: starting TikaService");
         ctx.ImportEngine.ProcessHostCollection.EnsureStarted(this.processName);
         if (pingUrl == null)
         {
            ctx.ImportLog.Log("No pingurl specified. Just waiting for 10s.");
            Thread.Sleep(10000);
            return;
         }

         ctx.ImportLog.Log(_LogType.ltTimer, "tika: service starting, waiting for ping reply.");
         DateTime limit = DateTime.UtcNow.AddMilliseconds(pingTimeout);
         Uri uri = new Uri(pingUrl);
         Exception saved = null;
         using (WebClient client = new WebClient())
         {
            while (true)
            {
               try
               {
                  client.DownloadData(uri);
                  ctx.ImportLog.Log(_LogType.ltTimerStop, "tika: up&running");
                  return;
               }
               catch (Exception err)
               {
                  saved = err;
               }
               if (DateTime.UtcNow >= limit) break;
            }
            throw new BMException(saved, "Tika service did not startup within {0}ms. LastErr={1}", pingTimeout, saved.Message);
         }
      }
   }


   public class TikaSecurityAccount : SecurityAccount, IComparable<TikaSecurityAccount>
   {
      public readonly String ExportedName;
      internal protected TikaSecurityAccount(SecurityCache parent, IdentityReference ident): base (parent, ident)
      {
         ExportedName = WellKnownSid == null ? base.Sid.Value : ((WellKnownSidType)WellKnownSid).ToString();
      }

      public override bool Equals(object obj)
      {
         if (obj==this) return true;

         TikaSecurityAccount other = obj as TikaSecurityAccount;
         if (other == null) return false;
         return ExportedName == other.ExportedName;
      }

      public override string ToString()
      {
         return ExportedName;
      }

      public static new SecurityAccount FactoryImpl(SecurityCache parent, IdentityReference ident)
      {
         return new TikaSecurityAccount(parent, ident);
      }

      public int CompareTo(TikaSecurityAccount other)
      {
         if (other == null) return 1;
         return ExportedName.CompareTo(other.ExportedName);
      }
   }
}
