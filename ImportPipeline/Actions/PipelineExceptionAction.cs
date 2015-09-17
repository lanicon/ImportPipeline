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
using System.Text.RegularExpressions;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;

namespace Bitmanager.ImportPipeline
{
   public class PipelineExceptionAction : PipelineAction
   {
      protected String msg;
      public PipelineExceptionAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         msg = node.ReadStr("@msg", "Exception requested by action.");
      }
      public PipelineExceptionAction(String name) : base(name) { }

      internal PipelineExceptionAction(PipelineExceptionAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.msg = optReplace(regex, name, template.msg);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         throw new Exception(msg);
      }
   }


   public class PipelineExceptionTemplate : PipelineTemplate
   {
      public PipelineExceptionTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineExceptionAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineExceptionAction((PipelineExceptionAction)template, key, regex);
      }
   }

}
