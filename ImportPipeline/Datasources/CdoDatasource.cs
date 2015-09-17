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
   public class CdoDatasource : StreamDatasourceBase
   {
      public CdoDatasource() : base(true, true) { }

      protected override void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm)
      {
         CDO.IMessage msg = new CDO.Message();
         msg.DataSource.OpenObject(new IStreamFromStream(strm), "IStream");
         sink.HandleValue(ctx, "record/subject", msg.Subject);
         sink.HandleValue(ctx, "record/bcc", msg.BCC);
         sink.HandleValue(ctx, "record/cc", msg.CC);
         sink.HandleValue(ctx, "record/from", msg.From);
         sink.HandleValue(ctx, "record/to", msg.To);
         Utils.FreeAndNil(ref msg);
         sink.HandleValue(ctx, "record", null);
      }

   }
}
