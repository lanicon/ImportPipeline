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

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline.Template
{

   public class TemplateFactory : ITemplateFactory
   {
      private IVariables initialVars;

      public virtual IVariables InitialVariables
      {
         get
         {
            if (initialVars == null) initialVars = new Variables();
            return initialVars;
         }
         set
         {
            initialVars = value;
         }
      }
      public virtual int DebugLevel { get; set; }
      public virtual bool AutoWriteGenerated { get; set; }
      public virtual ITemplateEngine CreateEngine()
      {
         return new TemplateEngine(this);
      }


      public TemplateFactory() { }

      /// <summary>
      /// Factory for template engines.
      /// Can have a constructor with these 2 parms, or a constructor with only ImportEngine, or no parms at all...
      /// </summary>
      public TemplateFactory(ImportEngine engine, XmlHelper xml) { }

   }
}
