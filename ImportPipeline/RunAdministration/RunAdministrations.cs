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
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{

   public class RunAdministrations : IEnumerable<RunAdministration>
   {
      protected List<RunAdministration> list;
      public readonly int Capacity;

      public RunAdministrations(int capacity = 100)
      {
         list = new List<RunAdministration>();
         this.Capacity = capacity;
      }

      public int Count
      {
         get { return list.Count; }
      }

      public RunAdministration this[int index]
      {
         get
         {
            return list[index];
         }
      }

      public void Load(String fn)
      {
         using (var fs = IOUtils.CreateInputStream(fn))
         {
            Load(JObject.Load(fs.CreateJsonReader()));
         }
      }
      public void Load(JObject root)
      {
         var arr = root.ReadArr("runs");
         var newList = new List<RunAdministration>(arr.Count);
         for (int i = 0; i < arr.Count; i++)
         {
            newList.Add(new RunAdministration((JObject)arr[i]));
         }
         newList.Sort(cbSortDate_Source);
         list = _shrink(newList, Capacity);
      }

      public void Save(String fn)
      {
         using (var fs = IOUtils.CreateOutputStream(fn))
         {
            var wtr = fs.CreateJsonWriter(Newtonsoft.Json.Formatting.Indented);
            ToJson().WriteTo(wtr);
            wtr.Flush();
         }

      }

      public JObject ToJson()
      {
         var arr = new JArray();
         foreach (var a in list)
         {
            arr.Add(a.ToJson());
         }
         var ret = new JObject();
         ret.Add("runs", arr);
         return ret;
      }

      public void Add(RunAdministration item)
      {
         list.Add(item);
      }

      public void Clear()
      {
         list.Clear();
      }

      public RunAdministrations Sort()
      {
         list.Sort(cbSortDate_Source);
         return this;
      }

      public RunAdministrations ShrinkToCapacity(int capacity = -1)
      {
         _shrink(list, capacity < 0 ? this.Capacity : capacity);
         return this;
      }

      private static List<RunAdministration> _shrink(List<RunAdministration> list, int capacity)
      {
         if (list.Count > capacity) list.RemoveRange(capacity, list.Count - capacity);
         return list;
      }

      public int IndexOf(RunAdministration item)
      {
         return list.IndexOf(item);
      }

      public bool Contains(RunAdministration item)
      {
         return list.Contains(item);
      }

      public RunAdministration GetLastOKRun(String ds)
      {
         RunAdministration ret = null;
         foreach (var a in list)
         {
            if (!String.Equals(a.DataSource, ds, StringComparison.OrdinalIgnoreCase)) continue;
            if (a.State != _ErrorState.OK) continue;
            if (ret == null || ret.RunDateUtc < a.RunDateUtc)
               ret = a;
         }
         return ret;
      }

      public DateTime GetLastOKRunDate(DatasourceAdmin ds)
      {
         var a = GetLastOKRun(ds.Name);
         return a == null ? DateTime.MinValue : a.RunDateUtc;
      }

      public DateTime GetLastOKRunDateShifted(DatasourceAdmin ds)
      {
         var a = GetLastOKRun(ds.Name);
         return a == null ? DateTime.MinValue : a.RunDateUtc.AddSeconds(+ds.ShiftLastRuntime);
      }

      public RunAdministrations Dump(String reason = null, Logger lg = null)
      {
         if (lg == null) lg = Logs.DebugLog;
         lg.Log("Dumping {0} runs. Reason: {1}", list.Count, reason);
         foreach (var a in list)
            lg.Log("-- {0}", a);
         return this;
      }

      public IEnumerator<RunAdministration> GetEnumerator()
      {
         return list.GetEnumerator();
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return list.GetEnumerator();
      }

      private static int cbSortDate_Source(RunAdministration a, RunAdministration b)
      {
         int rc = Comparer<DateTime>.Default.Compare(b.RunDateUtc, a.RunDateUtc);
         if (rc != 0) return rc;
         return StringComparer.OrdinalIgnoreCase.Compare(a.DataSource, b.DataSource);
      }
   }


}
