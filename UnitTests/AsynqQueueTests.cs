using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;
using Bitmanager.ImportPipeline;
using System.Threading;
using Bitmanager.Core;
using System.Diagnostics;

namespace UnitTests
{
   [TestClass]
   public class AsynqQueueTests
   {
      [TestMethod]
      public void TestAsync()
      {
         int cnt = 1047;
         handleQueue(AsyncRequestQueue.Create(0), cnt);
         handleQueue(AsyncRequestQueue.Create(1), cnt);
         handleQueue(AsyncRequestQueue.Create(2), cnt);
         handleQueue(AsyncRequestQueue.Create(3), cnt);
         handleQueue(AsyncRequestQueue.Create(4), cnt);
      }

      private void handleQueue(AsyncRequestQueue q, int cnt)
      {
         Stopwatch w = new Stopwatch();
         w.Start();
         Dict<int, int> dict = new Dict<int, int>(cnt);
         for (int i = 0; i < cnt; i++)
         {
            dict.Add(i, -1);

            AsyncElt popped = (AsyncElt)q.PushAndOptionalPop(new AsyncElt(i));
            processPopped (dict, popped);
         }
         while (true)
         {
            AsyncElt popped = (AsyncElt)q.Pop();
            if (popped == null) break;
            processPopped(dict, popped);
         }

         Assert.AreEqual (cnt, dict.Count);

         foreach (var kvp in dict)
         {
            Assert.AreEqual(2 * kvp.Key, kvp.Value, "value for {0} was {1}", kvp.Key, kvp.Value);
         }
         Console.WriteLine("Elapsed: {0}ms for q={1}", w.ElapsedMilliseconds, q);
      }
      private void processPopped(Dict<int, int> dict, AsyncElt popped)
      {
         if (popped == null) return;
         Assert.AreEqual(2 * popped.req, popped.resp);
         int existing;
         Assert.IsTrue (dict.TryGetValue(popped.req, out existing), "elt {0} is missing", popped.req);
         Assert.AreEqual(-1, existing, "existing for {0} was {1}", popped.req, existing);
         dict[popped.req] = popped.resp;
      }

      private class AsyncElt : AsyncRequestElement
      {
         public int req;
         public int resp;

         public AsyncElt(int req)
         {
            this.req = req;
            action = dowork;
         }

         private void dowork(AsyncRequestElement elt)
         {
            Thread.Sleep(1);
            resp = 2 * req;
         }
      }
   }
}
