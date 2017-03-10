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
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Xml;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class WebStreamDirectory : StreamDirectory
   {
      public readonly List<WebStreamProvider> providers;
      public WebStreamDirectory(PipelineContext ctx, XmlElement providerNode, XmlElement parentNode): base (ctx, providerNode)
      {
         providers = new List<WebStreamProvider>();
         if (providerNode.HasAttribute("url"))
            providers.Add(new WebStreamProvider(ctx, providerNode, parentNode, this));
         else
         {
            foreach (XmlNode x in providerNode.SelectMandatoryNodes("url"))
            {
               providers.Add(new WebStreamProvider(ctx, x, providerNode, this));
            }
         }
      }

      public override IEnumerator<object> GetChildren(PipelineContext ctx)
      {
         foreach (var p in providers) yield return p;
      }

      public override string ToString()
      {
         return String.Format ("{0} [first url={1}]", GetType().Name, providers[0].Uri);
      }

   }
}