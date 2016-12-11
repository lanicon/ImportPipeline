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
using Newtonsoft.Json;
using Bitmanager.ImportPipeline.StreamProviders;
using System.Reflection;

namespace Bitmanager.ImportPipeline
{
   public class JsonDatasource : Datasource
   {
      private RootStreamDirectory streamDirectory;
      private int splitUntil;
      private bool objectPerLine;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         streamDirectory = new RootStreamDirectory(ctx, node);
         splitUntil = node.ReadInt("@splituntil", 1);
         objectPerLine = node.ReadBool("@objectperline", false);
      }


      private StringDict getAttributes(XmlNode node)
      {
         StringDict ret = new StringDict();
         if (node == null) return ret;
         var coll = node.Attributes;
         for (int i = 0; i < coll.Count; i++)
         {
            var att = coll[i];
            if (att.LocalName.Equals("url", StringComparison.InvariantCultureIgnoreCase)) continue;
            if (att.LocalName.Equals("baseurl", StringComparison.InvariantCultureIgnoreCase)) continue;
            ret[att.LocalName] = att.Value;
         }
         return ret;
      }

      private static ExistState toExistState(Object result)
      {
         if (result == null || !(result is ExistState)) return ExistState.NotExist;
         return (ExistState)result;
      }
      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         int splitUntil = elt.ContextNode.ReadInt("@splituntil", this.splitUntil);
         bool objectPerLine = elt.ContextNode.ReadBool("@objectperline", this.objectPerLine);

         ctx.SendItemStart(elt);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0)
            return;

         ExistState existState = ExistState.NotExist;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            existState = toExistState(sink.HandleValue(ctx, "record/_checkexist", null));
         }

         //Check if we need to convert this file
         if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
         {
            ctx.Skipped++;
            ctx.ImportLog.Log("Skipped: {0}. Date={1}", elt, 0);// dtFile);
            return;
         }

         List<String> keys = new List<string>();
         List<String> values = new List<String>();
         Stream fs = null;
         try
         {
            fs = elt.CreateStream();
            if (!this.objectPerLine)
               importRecord(ctx, sink, fs, splitUntil);
            else
            {
               byte[] buf = new byte[4096];
               int offset = 0;
               MemoryStream tmp = new MemoryStream();
               while (true)
               {
                  int len = offset + fs.Read (buf, offset, buf.Length-offset);
                  if (len == offset) break;
                  int i = offset;
                  for (; i<len; i++)
                  {
                     if (buf[i] == '\n') break;
                  }

                  tmp.Write(buf, offset, i - offset);
                  if (i==offset)
                  {
                     offset = 0;
                     continue;
                  }


                  if (tmp.Position > 0)
                  {
                     tmp.Position = 0;
                     importRecord(ctx, sink, tmp, splitUntil);
                     tmp.Position = 0;
                  }
                  if (i+1 < offset)
                     tmp.Write(buf, i+1, len-i-1);
               }
               if (offset > 0) tmp.Write(buf, 0, offset);
               if (tmp.Position > 0)
               {
                  tmp.Position = 0;
                  importRecord(ctx, sink, tmp, splitUntil);
               }
            }
            ctx.OptSendItemStop();
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }

      private void importRecord (PipelineContext ctx, IDatasourceSink sink, Stream strm, int splitUntil)
      {
         JsonTextReader rdr = new JsonTextReader  (new StreamReader (strm, true));
         JToken jt = JObject.ReadFrom(rdr);
         rdr.Close();
         strm.Close();

         if (jt.Type != JTokenType.Array)
         {
            Pipeline.EmitToken(ctx, sink, jt, "record", splitUntil);
            ctx.IncrementEmitted();
         }
         else
         {
            foreach (var item in (JArray)jt)
            {
               Pipeline.EmitToken(ctx, sink, item, "record", splitUntil);
               ctx.IncrementEmitted();
            }
         }
      }

      public static String WrapMessage (Exception ex, String sub, String fmt)
      {
         String msg = ex.Message;
         if (msg.IndexOf(sub) >= 0) return msg;
         return String.Format(fmt, msg, sub);
      }
      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in streamDirectory.GetProviders(ctx))
         {
            try
            {
               importUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, WrapMessage (e, elt.ToString(), "{0}\r\nUrl={1}."));
            }
         }
      }

   }
}
