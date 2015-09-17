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

using Bitmanager.Elastic;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline
{
   public class PostProcessors
   {
      StringDict<IPostProcessor> postProcessors;
      public PostProcessors(ImportEngine engine, XmlNode collNode)
      {
         postProcessors = new StringDict<IPostProcessor>();
         if (collNode == null) return;

         var nodes = collNode.SelectNodes("postprocessor"); 
         for (int i=0; i<nodes.Count; i++)
         {
            XmlNode c = nodes[i];
            IPostProcessor p = ImportEngine.CreateObject<IPostProcessor> (c, engine, c);
            postProcessors.Add(p.Name, p);
         }
      }

      public IPostProcessor GetPostProcessor(string processor)
      {
         IPostProcessor x;
         if (postProcessors.TryGetValue(processor, out x)) return x;

         throw new BMException("Postprocessor [{0}] not found.", processor);
      }
   }
}
