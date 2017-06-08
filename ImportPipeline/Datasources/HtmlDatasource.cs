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
using Bitmanager.ImportPipeline.StreamProviders;
using Bitmanager.ImportPipeline.Datasources;
using HtmlAgilityPack;
using System.IO;
using System.Xml.XPath;


namespace Bitmanager.ImportPipeline
{
   public class HtmlDatasource : StreamDatasourceBase
   {
      protected NodeSelector selector;
      public HtmlDatasource() : base(false, true)
      { }

      public override void Init(PipelineContext ctx, XmlNode node)
      {
         base.Init(ctx, node);
         selector = NodeSelector.Parse(node.SelectMandatoryNode("select"));
      }

      protected override void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm)
      {
         var doc = new HtmlDocument();
         doc.Load(strm, Encoding.UTF8); //fixme: detect encoding
         selector.Process(ctx, new HtmlNodeWrapper((HtmlNodeNavigator)doc.CreateNavigator()));
      }
   }

}
