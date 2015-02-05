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
            ImportEngine engine = new ImportEngine();
            engine.Load(xml);
            engine.ImportFlags = flags;
            engine.MaxAdds = maxAdds;
            engine.MaxEmits = maxEmits;
            return engine.Import(activeDS);
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
