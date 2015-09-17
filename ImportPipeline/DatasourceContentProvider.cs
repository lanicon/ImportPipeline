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

namespace Bitmanager.ImportPipeline
{
   public interface IDatasourceFeederElement
   {
      XmlNode Context {get;}
      Object  Element {get;}
   }
   public interface IDatasourceFeeder
   {
      void Init(PipelineContext ctx, XmlNode node);
      IEnumerable<IDatasourceFeederElement> GetElements (PipelineContext ctx);
   }


   public class FeederElementBase: IDatasourceFeederElement
   {
      public XmlNode Context { get; protected set; }
      public Object Element { get; protected set; }
      public FeederElementBase(XmlNode ctx, Object element)
      {
         Context = ctx;
         Element = element;
      }
      protected FeederElementBase(XmlNode ctx)
      {
         Context = ctx;
      }
      protected FeederElementBase()
      {
      }
      public override string ToString()
      {
         return Element.ToString();
      }
   }
}
