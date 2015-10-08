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
