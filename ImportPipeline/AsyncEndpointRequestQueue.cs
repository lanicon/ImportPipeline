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
      public readonly Object WhatToAdd;
      private IAsyncResult asyncResult;
      private Action<AsyncRequestElement> action;
      internal uint queuedOrder;  //Used by the Q
      private volatile bool endInvokeCalled;

      public AsyncRequestElement(Object whatToAdd, Action<AsyncRequestElement> action)
      {
         this.action = action;
         WhatToAdd = whatToAdd;
      }

      public bool IsCompleted
      {
         get
         {
            {
               return asyncResult != null && asyncResult.IsCompleted;
            }
         }
      }

      public void EndInvoke()
      {
         if (endInvokeCalled) return;
         endInvokeCalled = true; //Next statement might result in an exception!
         action.EndInvoke(asyncResult);
      }

      internal void Start()
      {
         Logs.DebugLog.Log("async start()");
         asyncResult = action.BeginInvoke(this, null, null);
         Logs.DebugLog.Log("async start()==>{0}", asyncResult);
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
      protected AsyncRequestQueue() { }
      /// <summary>
      /// If a request is added, it takes the place of an empty or ready request.
      /// If not found, the request takes the place of the oldest request in the Q, but before that, a wait is issued on the request to be removed.
      /// Adding an element to the Q implies calling BeginInvoke on the element that is added
      /// </summary>
      public abstract AsyncRequestElement Add(AsyncRequestElement req);

      /// <summary>
      /// Waits for all pending requests to complete
      /// </summary>
      public abstract void EndInvokeAll();

      /// <summary>
      /// Creates an optimized Q, depending on the size
      /// </summary>
      public static AsyncRequestQueue Create(int size)
      {
         return size > 1 ? (AsyncRequestQueue)new AsyncRequestQueueMulti(size) : new AsyncRequestQueueSingle();
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

      public override AsyncRequestElement Add(AsyncRequestElement req)
      {
         AsyncRequestElement elt;
         uint lowestOrder =uint.MaxValue;
            int lowestIdx=0;
         for (int i = 0; i < q.Length; i++)
         {
            elt = q[i];
            if (elt == null)
            {
               q[i] = req;
               goto OPTIONAL_START;
            }
            if (elt.IsCompleted)
            {
               elt.EndInvoke();
               q[i] = req;
               goto OPTIONAL_START;
            }
            if (elt.queuedOrder < lowestOrder)
            {
               lowestOrder = elt.queuedOrder;
               lowestIdx = i;
            }
         }

         q[lowestIdx].EndInvoke();
         q[lowestIdx] = req;
         
      OPTIONAL_START:
         req.queuedOrder = order++;
         req.Start();
         return req;
      }

      public override void EndInvokeAll()
      {
         for (int i = 0; i < q.Length; i++)
         {
            var elt = q[i];
            q[i] = null;
            if (elt != null) elt.EndInvoke();
         }
      }
   }

   public class AsyncRequestQueueSingle : AsyncRequestQueue
   {
      private AsyncRequestElement q;

      public AsyncRequestQueueSingle()
      {
      }

      public override AsyncRequestElement Add(AsyncRequestElement req)
      {
         //dumpQ("before");
         if (q == null)
         {
            q = req;
            goto OPTIONAL_START;
         }
         if (q.IsCompleted)
         {
            q.EndInvoke();
            q = req;
            goto OPTIONAL_START;
         }

         q.EndInvoke();
         q = req;

      OPTIONAL_START:
         req.Start();
         //dumpQ("after");
         return req;
      }

      public override void EndInvokeAll()
      {
         var elt = q;
         q = null;
         if (elt != null) elt.EndInvoke();
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
}
