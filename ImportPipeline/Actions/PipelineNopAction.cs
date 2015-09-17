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
   public class PipelineNopAction : PipelineAction
   {
      public PipelineNopAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }
      public PipelineNopAction(String name) : base(name) { }

      internal PipelineNopAction(PipelineNopAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         return null;
      }
   }

   public class PipelineNopTemplate : PipelineTemplate
   {
      public PipelineNopTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineNopAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineNopAction((PipelineNopAction)template, key, regex);
      }
   }

}
