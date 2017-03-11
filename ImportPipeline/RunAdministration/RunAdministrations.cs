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
      private List<RunAdministration> list;
      public readonly RunAdministrationSettings Settings;

      public RunAdministrations(RunAdministrationSettings settings)
      {
         this.Settings = settings;
         list = new List<RunAdministration>();
         Load();
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

      public void Merge (RunAdministrations other)
      {
         Dump("before dst");
         other.Dump("before src");
         if (other == null || other.list.Count == 0) return;

         if (list.Count == 0)
         {
            list.AddRange(other.list);
            return;
         }

         foreach (var a in other.list) 
            Add(a);
         Dump("after merge");
      }

      public void Load()
      {
         if (Settings.FileName != null) Load(Settings.FileName);
         OptDump("Load");
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
         for (int i = 0; i < arr.Count; i++)
         {
            Add(new RunAdministration((JObject)arr[i]));
         }
      }

      public RunAdministrations OptDump(String reason)
      {
         if (Settings.Dump != 0) Dump(reason);
         return this;
      }

      public RunAdministrations Dump(String reason)
      {
         Logger lg = Settings.Engine.ImportLog;
         int N = Settings.Dump;
         if (N <= 0) N = list.Count;
         else N = Math.Min(N, list.Count);

         lg.Log("Dumping top {0} of {1} import admins: {2}", N, list.Count, reason);
         for (int i=0; i<N; i++)
         {
            var x = list[i].ToJson();
            lg.Log("-- {0}", x.ToString(Newtonsoft.Json.Formatting.None));
         }
         return this;
      }

      public void Save()
      {
         OptDump("Save");
         if (Settings.FileName != null) Save(Settings.FileName);
      }
      public void Save(String fn)
      {
         using (var fs = IOUtils.CreateOutputStream(fn))
         {
            var wtr = fs.CreateJsonWriter(Newtonsoft.Json.Formatting.None);
            wtr.WriteStartObject();
            wtr.WriteStartArray("runs");
            wtr.WriteRaw("\r\n ");
            foreach (var a in list)
            {
               a.ToJson().WriteTo(wtr);
               wtr.WriteRaw("\r\n");
            }
            wtr.WriteEndArray();
            wtr.WriteEndObject();
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
         int i=0;
         int rc = -1;
         for (; i<list.Count; i++)
         {
            rc = StringComparer.OrdinalIgnoreCase.Compare (item.DataSource, list[i].DataSource);
            if (rc <= 0) break;
         }
         
         int j;
         for (j = i; j < list.Count; j++)
         {
            rc = StringComparer.OrdinalIgnoreCase.Compare(item.DataSource, list[j].DataSource);
            if (rc != 0) break;
            if (item.RunDateUtc == list[j].RunDateUtc) goto EXIT_RTN;
            if (item.RunDateUtc > list[j].RunDateUtc) break;
         }

         int k;
         for (k = j; k < list.Count; k++)
         {
            rc = StringComparer.OrdinalIgnoreCase.Compare(item.DataSource, list[k].DataSource);
            if (rc != 0) break;
         }

         /* State:
          *   i= start of ds
          *   j= where to insert
          *   k= end of ds
          */
         if (k - i < Settings.Capacity)
            goto INSERT_J;

         //We are over capacity

         //Process incr imports
         if ((item.ImportFlags & _ImportFlags.ImportFull) == 0)
         {
            if (j == k)
               goto EXIT_RTN;

            //Try to remove an incr behind the insertion point
            for (int x = k - 1; x >= j; x--)
            {
               if ((list[x].ImportFlags & _ImportFlags.ImportFull) == 0)
               {
                  k = x;
                  goto REMOVE_K_INSERT_J;
               }
            }

            //We should ignore this one
            goto EXIT_RTN;
         }

         //Process a full import admin

         //Try to remove the last incr
         for (int x = k - 1; x > i; x--)
         {
            if ((list[x].ImportFlags & _ImportFlags.ImportFull) == 0)
            {
               k = x;
               goto REMOVE_K_INSERT_J;
            }
         }

         //We should ignore this one
         goto EXIT_RTN;

      REMOVE_K_INSERT_J: 
         if (k==j)
         {
            list[j] = item;
            goto EXIT_RTN;
         }
      if (k < j) j--;
         list.RemoveAt(k);

      INSERT_J: 
         list.Insert(j, item);

      EXIT_RTN:
         return;
      }

      public void Clear()
      {
         list.Clear();
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
   }

}
