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
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline
{
   public class PipelineEmitAction : PipelineAction
   {
      enum Destination { PipeLine = 1, Datasource = 2 };

      private String eventKey;
      private String recField;
      private int maxLevel;
      private Destination destination;
      public PipelineEmitAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         eventKey = node.ReadStr("@emitexisting");
         destination = node.ReadEnum("@destination", Destination.PipeLine);
         maxLevel = node.ReadInt("@maxlevel", 1);
         recField = node.ReadStr("@emitfield", null);
      }

      internal PipelineEmitAction(PipelineEmitAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.eventKey = optReplace(regex, name, template.eventKey);
         this.recField = optReplace(regex, name, template.recField);
         this.destination = template.destination;
         this.maxLevel = template.maxLevel;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         IDatasourceSink sink = ctx.Pipeline;
         if (destination == Destination.Datasource) sink = (IDatasourceSink)ctx.DatasourceAdmin.Datasource;
         String reckey = (String)ctx.Pipeline.GetVariable("key");
         if (reckey == null) return null;

         this.endPoint.EmitRecord(ctx, reckey, recField, sink, eventKey, maxLevel);
         return value;
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", eventKey={0}, dest={1}, maxlevel={2}", eventKey, destination, maxLevel);
      }
   }

   public class PipelineEmitTemplate : PipelineTemplate
   {
      public PipelineEmitTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineEmitAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineEmitAction((PipelineEmitAction)template, key, regex);
      }
   }
}
