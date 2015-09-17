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
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Determines how a key should be compared
   /// </summary>
   public enum CompareType { String = 0x01, Int = 0x02, Long = 0x04, Double = 0x08, Date = 0x10, Ascending=0, Descending = 0x10000, CaseInsensitive = 0x20000 };

   public class KeyAndType
   {
      public readonly JPath Key;
      public readonly CompareType Type;

      public KeyAndType(JPath k, CompareType t)
      {
         Key = k;
         Type = t;
      }
      public KeyAndType(String k, CompareType t)
      {
         Key = new JPath(k);
         Type = t;
      }
      public KeyAndType(XmlNode node)
      {
         Key = new JPath(node.ReadStr("@expr"));
         Type = node.ReadEnum<CompareType>("@type"); 
      }

      public static List<KeyAndType> CreateKeyList(XmlNode root, String name, bool mandatory)
      {
         XmlNodeList nodes = mandatory ? root.SelectMandatoryNodes(name) : root.SelectNodes(name);
         var ret = new List<KeyAndType>(nodes.Count);

         for (int i = 0; i < nodes.Count; i++)
            ret.Add(new KeyAndType(nodes[i]));
         return ret;
      }

   }
}
