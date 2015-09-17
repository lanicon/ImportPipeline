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
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline.Template
{
   public interface ITemplateSettings
   {
      IVariables InitialVariables { get; set; }
      int DebugLevel { get; set; }
      bool AutoWriteGenerated { get; set; }
      ITemplateSettings Clone();
   }

   public class TemplateSettings : ITemplateSettings
   {
      public virtual IVariables InitialVariables { get; set; }
      public virtual int DebugLevel { get; set; }
      public virtual bool AutoWriteGenerated { get; set; }
      public virtual ITemplateSettings Clone() { return new TemplateSettings(this); }

      public TemplateSettings() { }
      public TemplateSettings(bool dump) { AutoWriteGenerated = dump; }
      public TemplateSettings(int dbgLevel) { DebugLevel = dbgLevel; }
      public TemplateSettings(bool dump, int dbgLevel) { AutoWriteGenerated = dump; DebugLevel = dbgLevel; }
      public TemplateSettings(TemplateSettings other)
      {
         InitialVariables = other.InitialVariables;
         DebugLevel = other.DebugLevel;
         AutoWriteGenerated = other.AutoWriteGenerated;
      }

   }
}
