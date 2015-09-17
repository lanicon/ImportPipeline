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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class MissedAdmin
   {
      StringDict<bool> dict;

      public MissedAdmin()
      {
      }

      public void AddMissed(String x, bool touched= false)
      {
         if (x == null) return;

         x = x.ToLowerInvariant();
         if (dict == null)
         {
            dict = new StringDict<bool>();
            dict.Add(x, touched);
            return;
         }
         if (dict.ContainsKey (x)) return;
         dict.Add(x, touched);
      }

      public void Combine(MissedAdmin other)
      {
         if (other.dict == null) return;
         foreach (var kvp in other.dict)
         {
            AddMissed(kvp.Key, kvp.Value);
         }
      }
   }
}
