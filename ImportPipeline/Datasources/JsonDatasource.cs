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
//using System.Threading.Tasks;
using System.Xml;
using System.Net;
using System.IO;
using HtmlAgilityPack;
using System.Text.RegularExpressions;
using System.Web;
using System.Threading;
using Bitmanager.Elastic;
using Newtonsoft.Json;
using Bitmanager.ImportPipeline.StreamProviders;

namespace Bitmanager.ImportPipeline
{
   public class JsonDatasource : Datasource
   {
      private GenericStreamProvider streamProvider;
      private int splitUntil;
      private bool dumpReader;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         streamProvider = new GenericStreamProvider(ctx, node);
         dumpReader = node.ReadBool("@debug", false);
         splitUntil = node.ReadInt("@splituntil", 1);
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
         StringDict attribs = getAttributes(elt.ContextNode);
         String fileName = elt.FullName;
         sink.HandleValue(ctx, "_start", fileName);
         //DateTime dtFile = File.GetLastWriteTimeUtc(fileName);
         //sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         sink.HandleValue(ctx, "record/virtualFilename", elt.VirtualName);

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
            JsonTextReader rdr = new JsonTextReader  (new StreamReader (fs, true));
            JObject obj = (JObject)JObject.ReadFrom(rdr);
            rdr.Close();
            fs.Close();

            Pipeline.EmitToken (ctx, sink, obj, "record", splitUntil);
            ctx.IncrementEmitted();
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
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
         foreach (var elt in streamProvider.GetElements(ctx))
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
