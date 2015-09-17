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
using Bitmanager.ImportPipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.Importer
{
   public class EngineWrapper : MarshalByRefObject
   {
      public ImportReport Run(_ImportFlags flags, String xml, String[] activeDS, int maxAdds, int maxEmits)
      {
         try
         {
            using (ImportEngine engine = new ImportEngine())
            {
               engine.ImportFlags = flags;
               engine.Load(xml);
               engine.MaxAdds = maxAdds;
               engine.MaxEmits = maxEmits;
               return engine.Import(activeDS);
            }
         }
         catch (Exception e)
         {
            Logs.ErrorLog.Log(e);
            throw new Exception(e.Message);
         }
      }
   }

   public class AsyncAdmin : IDisposable
   {
      public ImportReport Report;
      AppDomain domain;
      Func<_ImportFlags, String, String[], int, int, ImportReport> action;
      IAsyncResult asyncResult;
      bool started; 

      public void Start(_ImportFlags flags, String xml, String[] activeDS, int maxRecords, int maxEmits)
      {
         domain = AppDomain.CreateDomain("import");
         Type type = typeof(EngineWrapper);

         EngineWrapper wrapper = (EngineWrapper)domain.CreateInstanceAndUnwrap(type.Assembly.FullName, type.FullName, false, BindingFlags.CreateInstance, null, null, Invariant.Culture, null);
         action = wrapper.Run;

         asyncResult = action.BeginInvoke(flags, xml, activeDS, maxRecords, maxEmits, null, null);
         started = true;
         return;
      }

      public void Stop()
      {
         if (asyncResult == null) return;
         IAsyncResult ar = asyncResult;
         asyncResult = null;

         Report = action.EndInvoke(ar);
      }

      public bool CheckStopped()
      {
         if (asyncResult == null) return started;
         return asyncResult.IsCompleted;
      }

      public void Cancel()
      {
         if (domain == null) return;
         AppDomain.Unload (domain);
         domain = null;
      }

      public void Dispose()
      {
         Cancel();
      }
   }
}
