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
   public class UniqueProcessor : PostProcessorBase
   {
      private readonly JComparer undupper;
      SortedDictionary<JToken[], bool> dict; 

      public UniqueProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("undupper"), "key", true);
         undupper = JComparer.Create(list);
      }

      public UniqueProcessor(PipelineContext ctx, UniqueProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         undupper = other.undupper;
      }


      public override IPostProcessor Clone(PipelineContext ctx, IDataEndpoint epOrnextProcessor)
      {
         return new UniqueProcessor(ctx, this, epOrnextProcessor);
      }

      public override string ToString()
      {
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("{0} [type={1}, clone=#{2}, undup={3}]",
            Name, GetType().Name, InstanceNo, undupper);
         return sb.ToString();
      }


      public override void Add(PipelineContext ctx)
      {
         if (accumulator.Count > 0)
         {
            ++cnt_received;
            JToken[] keys = undupper.GetKeys(accumulator);
            if (dict==null)
            {
               dict = new SortedDictionary<JToken[], bool>(new KeysComparer(undupper));
               dict.Add(keys, false);
            }
            else
            {
               if (dict.ContainsKey (keys)) return;
               dict.Add(keys, false);
            }
            PassThrough (ctx, accumulator);
            Clear();
         }
      }

      class KeysComparer: IComparer<JToken[]> 
      {
         private readonly JComparer comparer;
         public KeysComparer(JComparer comparer)
         {
            this.comparer = comparer;
         }
         public int Compare(JToken[] x, JToken[] y)
         {
            return comparer.CompareKeys(x, y);
         }
      }
      //class KeysHashComparer: IEqualityComparer<JToken[]>
      //{
      //   private readonly JComparer comarer;


      //   public bool Equals(JToken[] x, JToken[] y)
      //   {
      //      throw new NotImplementedException();
      //   }

      //   public int GetHashCode(JToken[] obj)
      //   {
      //      throw new NotImplementedException();
      //   }
      //}
   }
}
