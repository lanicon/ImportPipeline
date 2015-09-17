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
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.IO;
using System.Globalization;
using Bitmanager.Elastic;
using Newtonsoft.Json;

namespace Bitmanager.ImportPipeline
{
   public class JsonEndpoint : Endpoint
   {
      public readonly int CacheSize;
      public readonly String LineSeparator;
      public readonly String FileName;
      public readonly Newtonsoft.Json.Formatting Formatting;


      private FileStream fs;
      private StreamWriter wtr;
      private JsonWriter jsonWtr;

      public JsonEndpoint(ImportEngine engine, XmlNode node)
         : base(engine, node)
      {
         FileName = engine.Xml.CombinePath(node.ReadStr("@file"));
         LineSeparator = node.ReadStr("@linesep", "\r\n");
         Formatting = node.ReadBool("@formatted", false) ? Newtonsoft.Json.Formatting.Indented : Newtonsoft.Json.Formatting.None;
      }

      protected override void Open(PipelineContext ctx)
      {
         fs = new FileStream(FileName, FileMode.Create, FileAccess.Write, FileShare.Read);
         wtr = new StreamWriter(fs, Encoding.UTF8, 32*1024);
         jsonWtr = new Newtonsoft.Json.JsonTextWriter(wtr);
         jsonWtr.Culture = Invariant.Culture;
         jsonWtr.Formatting = Formatting;
      }
      protected override void Close(PipelineContext ctx)
      {
         logCloseAndCheckForNormalClose(ctx);
         if (jsonWtr != null)
         {
            jsonWtr.Flush();
            jsonWtr = null;
         }
         if (wtr != null)
         {
            wtr.Flush();
            wtr = null;
         }
         if (fs != null)
         {
            fs.Close();
            fs = null;
         }
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName, bool mustExcept)
      {
         return new JsonDataEndpoint(this);
      }


      internal void WriteAccumulator(JObject accu)
      {
         accu.WriteTo (jsonWtr);
         jsonWtr.WriteRaw(LineSeparator);
      }
   }


   public class JsonDataEndpoint : JsonEndpointBase<JsonEndpoint>
   {
      public JsonDataEndpoint(JsonEndpoint endpoint)
         : base(endpoint)
      {
      }

      public override void Add(PipelineContext ctx)
      {
         if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0)
         {
            ctx.DebugLog.Log("Add: accumulator.Count={0}", accumulator.Count);
         }
         if (accumulator.Count == 0) return;
         OptLogAdd();
         Endpoint.WriteAccumulator(accumulator);
         Clear();
      }
   }

}
