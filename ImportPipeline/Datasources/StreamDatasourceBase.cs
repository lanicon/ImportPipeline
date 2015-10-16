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
using System.IO;
using Bitmanager.Core;
using Bitmanager.Json;
using Bitmanager.IO;
using Bitmanager.Xml;
using Bitmanager.ImportPipeline.StreamProviders;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public abstract class StreamDatasourceBase : Datasource
   {
      protected GenericStreamProvider streamProvider;
      protected Encoding encoding;
      protected int splitUntil;
      protected bool logSkips;
      protected bool addEmitted;

      protected StreamDatasourceBase(bool logSkips, bool addEmitted)
      {
         this.logSkips = logSkips;
         this.addEmitted = addEmitted;
      }


      public virtual void Init(PipelineContext ctx, XmlNode node)
      {
         Init(ctx, node, Encoding.UTF8);
      }
      public virtual void Init(PipelineContext ctx, XmlNode node, Encoding defEncoding)
      {
         streamProvider = new GenericStreamProvider(ctx, node);
         String enc = node.ReadStr("@encoding", null);
         encoding = enc == null ? defEncoding : Encoding.GetEncoding(enc);
         logSkips = node.ReadBool("@logskips", logSkips);
         splitUntil = node.ReadInt("@splituntil", splitUntil);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in streamProvider.GetElements(ctx))
         {
            try
            {
               ImportUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               e = new BMException(e, WrapMessage(e, elt.ToString(), "{0}\r\nUrl={1}."));
               ctx.HandleException(e);
            }
         }
      }

      public static String WrapMessage(Exception ex, String sub, String fmt)
      {
         String msg = ex.Message;
         if (msg.IndexOf(sub) >= 0) return msg;
         return String.Format(fmt, msg, sub);
      }

      public static ExistState toExistState(Object result)
      {
         if (result == null) return ExistState.NotExist;

         if (result is ExistState)
            return (ExistState)result;

         if (result is bool)
            return ((bool)result) ? ExistState.Exist : ExistState.NotExist;
         return ExistState.NotExist;
      }

      protected virtual bool CheckNeedImport (PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         ExistState existState = toExistState(sink.HandleValue(ctx, "record/_checkexist", elt));

         //return true if we need to convert this file
         return ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) == 0);
      }

      protected abstract void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm);

      protected virtual void ImportUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         int orgEmitted = ctx.Emitted;
         if (addEmitted)
            ctx.IncrementEmitted();
         DateTime dtFile = elt.LastModified;
         sink.HandleValue(ctx, "record/_start", elt);
         sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         sink.HandleValue(ctx, "record/filename", elt.FullName);
         sink.HandleValue(ctx, "record/virtualfilename", elt.VirtualName);

         //Check if we need to import this file
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            if (!CheckNeedImport(ctx, sink, elt)) goto SKIPPED;
         }
         if (ctx.SkipUntilKey == "record") goto SKIPPED;

         using (Stream fs = elt.CreateStream())
         {
            ImportStream(ctx, sink, elt, fs);
         }
         return;

      SKIPPED:
         ctx.Skipped++;
         if (!addEmitted && orgEmitted == ctx.Emitted) ++ctx.Emitted;
         if (logSkips) ctx.DebugLog.Log("Skipped: {0}. Date={1}", elt.FullName, elt.LastModified);
      }
   }
}
