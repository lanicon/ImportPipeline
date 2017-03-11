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

using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class RunAdministrationSettings
   {
      public const int DEF_CAPACITY = 100;
      public readonly String FileName;
      public readonly ImportEngine Engine;
      public readonly int Capacity;
      public readonly int Dump;

      public RunAdministrationSettings(ImportEngine engine, XmlNode node)
      {
         Engine = engine;
         if (node == null)
         {
            FileName = null;
            Capacity = DEF_CAPACITY;
         }
         else
         {
            FileName = node.ReadPath("@file", null);
            Capacity = node.ReadInt("@capacity", DEF_CAPACITY);
            Dump = node.ReadInt("@dump", 0);
         }
      }
      public RunAdministrationSettings(ImportEngine engine, String fn, int cap, int dump)
      {
         Engine = engine;
         FileName = fn;
         Capacity = cap;
         Dump = dump;
      }

      public RunAdministrations Load()
      {
         return new RunAdministrations(this);
      }

      public void Save(RunAdministrations a)
      {
         a.Save ();
      }
   }
}
