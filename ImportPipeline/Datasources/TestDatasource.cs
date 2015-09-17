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
using Bitmanager.Elastic;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System.Xml;
using System.Net;

namespace Bitmanager.ImportPipeline
{
   public class TestDatasource : Datasource
   {
      public void Init(PipelineContext ctx, XmlNode node)
      {
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         sink.HandleValue(ctx, "record/double", 123.45);
         sink.HandleValue(ctx, "record/date", DateTime.Now);
         sink.HandleValue(ctx, "record/utcdate", DateTime.UtcNow);
         sink.HandleValue(ctx, "record/int", -123);
         sink.HandleValue(ctx, "record/string", "foo bar");
         sink.HandleValue(ctx, "record", null);
      }
   }
}
