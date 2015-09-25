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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline.Template
{

   public class Variables : IVariables
   {
      private StringDict<Object> vars;

      public Variables()
      {
         vars = new StringDict<object>();
      }

      public void Clear()
      {
         vars.Clear();
      }

      public void Set(string key, object value)
      {
         vars[key] = value;
      }

      public object Get(string key)
      {
         Object o;
         return vars.TryGetValue(key, out o) ? o : null;
      }

      public IVariables Clone()
      {
         var result = new Variables ();
         result.copyFrom(this);
         return result;
      }

      protected virtual void copyFrom(Variables other)
      {
         foreach (var kvp in other)
         {
            vars.Add(kvp.Key, kvp.Value);
         }
      }

      public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
      {
         return vars.GetEnumerator();
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return vars.GetEnumerator();
      }

      public int Count
      {
         get { return vars.Count; }
      }
   }

   public static class VariablesExtensions
   {
      public static IVariables CopyFromDictionary(this IVariables v, IDictionary dict, bool mustOverride = true)
      {
         foreach (DictionaryEntry kvp in Environment.GetEnvironmentVariables())
         {
            v.Set(kvp.Key.ToString(), kvp.Value);
         }
         return v;
      }

      public static IVariables Dump(this IVariables v, Logger logger, String reason)
      {
         if (reason == null)
            logger.Log("Dumping {0} variables.", v.Count);
         else
            logger.Log("Dumping {0} variables. Reason={1}.", v.Count, reason);
         foreach (var kvp in v)
         {
            logger.Log("-- {0}={1}", kvp.Key, kvp.Value);
         }
         return v;
      }
   }

}
