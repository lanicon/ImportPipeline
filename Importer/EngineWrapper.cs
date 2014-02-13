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
      public void Run(_ImportFlags flags, String xml, String[] activeDS)
      {
         ImportEngine engine = new ImportEngine(flags);
         engine.Load(xml);
         engine.Import(activeDS);
      }
   }

   public class AsyncAdmin : IDisposable
   {
      AppDomain domain;
      Action<_ImportFlags, String, String[]> action;
      IAsyncResult asyncResult;
      bool started; 

      public void Start(_ImportFlags flags, String xml, String[] activeDS)
      {
         domain = AppDomain.CreateDomain("import");
         Type type = typeof(EngineWrapper);

         EngineWrapper wrapper = (EngineWrapper)domain.CreateInstanceAndUnwrap(type.Assembly.FullName, type.FullName, false, BindingFlags.CreateInstance, null, null, Invariant.Culture, null);
         action = wrapper.Run;

         asyncResult = action.BeginInvoke(flags, xml, activeDS, null, null);
         started = true;
         return;
      }

      public void Stop()
      {
         if (asyncResult == null) return;
         IAsyncResult ar = asyncResult;
         asyncResult = null;

         action.EndInvoke(ar);
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
