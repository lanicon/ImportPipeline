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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Queue element that will be queued and executed async.
   /// </summary>
   public class AsyncRequestElement
   {
      public readonly Object Context;
      public Object Result;
      private IAsyncResult asyncResult;
      protected Action<AsyncRequestElement> action;
      public Exception Exception { get; private set; }
      internal uint queuedOrder;  //Used by the Q
      private volatile bool endInvokeCalled;
      private volatile bool completed;

      public AsyncRequestElement(Object whatToAdd, Action<AsyncRequestElement> action)
      {
         this.action = action;
         Context = whatToAdd;
      }
      protected AsyncRequestElement()
      { }

      public bool IsCompleted
      {
         get
         {
            {
               return completed || (asyncResult != null && asyncResult.IsCompleted);
            }
         }
      }

      public void EndInvoke()
      {
         if (endInvokeCalled) return;
         endInvokeCalled = true; //Next statement might result in an exception!
         action.EndInvoke(asyncResult);
      }

      internal void Run()
      {
         completed = true;
         action(this);
      }

      internal AsyncRequestElement Start(uint order)
      {
         queuedOrder = order;
         //Logs.DebugLog.Log("async start()");
         asyncResult = action.BeginInvoke(this, null, null);
         //Logs.DebugLog.Log("async start()==>{0}", asyncResult);
         return this;
      }

      public override string ToString()
      {
         return String.Format("{0} (asyncR={1}, order={2}, endinvoked={3}, completed={4})", this.GetType().Name, asyncResult, queuedOrder, endInvokeCalled, IsCompleted);
      }
   }


   /// <summary>
   /// Queue that determines how many requests are executed in parallel
   /// If more than this max requests are to be queued, a wait willbe done on one of the previous requests
   /// </summary>
   public abstract class AsyncRequestQueue
   {
      protected static readonly List<AsyncRequestElement> EMPTY = new List<AsyncRequestElement>();
      protected AsyncRequestQueue() { }
      /// <summary>
      /// If a request is added, it takes the place of an empty or ready request.
      /// If not found, the request takes the place of the oldest request in the Q, but before that, a wait is issued on the request to be removed.
      /// Adding an element to the Q implies calling BeginInvoke on the element that is added
      /// The removed item is returned (or null if there was nothing to remove)
      /// </summary>
      public abstract AsyncRequestElement PushAndOptionalPop(AsyncRequestElement req);

      /// <summary>
      /// Waits for an item to complete and returns the completed item
      /// If the queue was empty, null is returned.
      /// </summary>
      public abstract AsyncRequestElement Pop();

      /// <summary>
      /// Waits for all pending requests to complete, pops them, and return them in a list
      /// </summary>
      public abstract List<AsyncRequestElement> PopAll();

      /// <summary>
      /// Waits for all pending requests to complete, pops them, and swallows exceptions
      /// </summary>
      public virtual Exception PopAllWithoutException()
      {
         Exception ret = null;
         while (true)
         {
            try { if (null == Pop()) break; }
            catch (Exception e) { if (ret == null) ret = e; }
         }
         return ret;
      }


      /// <summary>
      /// Creates an optimized Q, depending on the size
      /// </summary>
      public static AsyncRequestQueue Create(int size)
      {
         if (size==0) return new AsyncRequestQueueZero();
         if (size>1) return new AsyncRequestQueueMulti(size);
         return new AsyncRequestQueueSingle();
      }
   }

   public class AsyncRequestQueueMulti : AsyncRequestQueue
   {
      private AsyncRequestElement[] q;
      private uint order;

      public AsyncRequestQueueMulti(int size)
      {
         q = new AsyncRequestElement[size];
      }

      public override string ToString()
      {
         return base.ToString() + "[size=" + q.Length + "]";
      }
      public override AsyncRequestElement PushAndOptionalPop(AsyncRequestElement req)
      {
         AsyncRequestElement popped = null;
         uint lowestRunningOrder = uint.MaxValue;
         int lowestRunningIdx = -1;
         uint lowestCompletedOrder = uint.MaxValue;
         int lowestCompletedIdx = -1;

         for (int i = 0; i < q.Length; i++)
         {
            popped = q[i];
            if (popped == null)
            {
               q[i] = req.Start(order++);
               return null;
            }
            if (popped.IsCompleted)
            {
               if (popped.queuedOrder >= lowestCompletedOrder) continue;
               lowestCompletedIdx = i;
               continue;
            }
            if (popped.queuedOrder >= lowestRunningOrder) continue;
            lowestRunningIdx = i;
         }

         //Pop and replace still running item
         int popIdx = lowestCompletedIdx;
         if (popIdx < 0) popIdx = lowestRunningIdx;
         popped = q[popIdx];
         q[popIdx] = null;

         try
         {
            popped.EndInvoke();
         }
         finally
         {
            if (req != null)  q[popIdx] = req.Start(order++);
         }
         return popped;
      }

      public override AsyncRequestElement Pop()
      {
         AsyncRequestElement popped = null;
         uint lowestRunningOrder = uint.MaxValue;
         int lowestRunningIdx = -1;
         uint lowestCompletedOrder = uint.MaxValue;
         int lowestCompletedIdx = -1;
         for (int i = 0; i < q.Length; i++)
         {
            popped = q[i];
            if (popped == null) continue;
            if (popped.IsCompleted)
            {
               if (popped.queuedOrder >= lowestCompletedOrder) continue;
               lowestCompletedIdx = i;
               continue;
            }
            if (popped.queuedOrder >= lowestRunningOrder) continue;
            lowestRunningIdx = i;
         }
         int popIdx = lowestCompletedIdx;
         if (lowestCompletedIdx >= 0) goto POP;
         popIdx = lowestRunningIdx;
         if (popIdx < 0) return null;

         POP:
         popped = q[popIdx];
         q[popIdx] = null;
         popped.EndInvoke();
         return popped;
      }

      public override List<AsyncRequestElement> PopAll()
      {
         List<AsyncRequestElement> ret = null;
         for (int i = 0; i < q.Length; i++)
         {
            AsyncRequestElement popped = q[i];
            if (popped == null) continue;
            q[i] = null;
            if (ret == null) ret = new List<AsyncRequestElement>();
            popped.EndInvoke();
            ret.Add(popped);
         }
         return ret == null ? EMPTY : ret;
      }

   }

   public class AsyncRequestQueueSingle : AsyncRequestQueue
   {
      private AsyncRequestElement q;

      public override AsyncRequestElement PushAndOptionalPop (AsyncRequestElement req)
      {
         AsyncRequestElement popped = q;
         q = null;
         //dumpQ("before");
         try
         {
            if (popped != null)
               popped.EndInvoke();
         }
         finally
         {
            if (req != null) q = req.Start(0);
         }
         //dumpQ("after");
         return popped;
      }

      public override AsyncRequestElement Pop()
      {
         var popped = q;
         q = null;
         if (popped != null) popped.EndInvoke();
         return popped;
      }

      public override List<AsyncRequestElement> PopAll()
      {
         var popped = q;
         q = null;
         if (popped == null) return EMPTY;
         popped.EndInvoke();
         return new List<AsyncRequestElement>(1) { popped };
      }


      private void dumpQ(String when)
      {
         Logs.DebugLog.Log ("Dumping SINGLE-Q {0}", when);
         if (q == null)
            Logs.DebugLog.Log("-- empty");
         else
            Logs.DebugLog.Log("-- {0}", q);
      }
   }

   public class AsyncRequestQueueZero : AsyncRequestQueue
   {
      public override AsyncRequestElement PushAndOptionalPop(AsyncRequestElement req)
      {
         req.Run();
         return req;
      }

      public override AsyncRequestElement Pop()
      {
         return null;
      }

      public override List<AsyncRequestElement> PopAll()
      {
         return EMPTY;
      }
   }

}
