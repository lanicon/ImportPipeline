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
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public abstract class MappedObjectEnumerator : IDisposable
   {
      protected String readerFile;
      protected int index;
      public MappedObjectEnumerator(String filename, int index)
      {
         this.index = index;
         //this.reader = rdr;
         this.readerFile = filename;
      }

      public abstract JObject GetNext();
      public abstract List<JObject> GetAll();
      public abstract void Close();

      public abstract void Dispose();
   }

   public abstract class MapperWritersBase : IDisposable
   {

      /// <summary>
      /// Writes the data to the appropriate file (designed by the hash)
      /// </summary>
      public abstract void Write(JObject data);


      /// <summary>
      /// Optional writes the data to the appropriate file (designed by the hash)
      /// If the index of the first key that had a null value >= minNullIndex, the value is not written.
      /// 
      /// The returnvalue reflects whether has been written or not.
      /// </summary>
      public abstract bool OptWrite(JObject data, int maxNullIndex = -1);

      public abstract void Dispose();

      public abstract MappedObjectEnumerator GetObjectEnumerator(int index, bool buffered = false);
   }
}
