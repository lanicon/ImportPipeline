using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Queue that determines how many requests are executed in parallel
   /// If more than this max requests are to be queued, a wait willbe done on one of the previous requests
   /// </summary>
   public class AsyncEndpointRequest
   {
      public readonly Object WhatToAdd;
      private IAsyncResult asyncResult;
      private Action<AsyncEndpointRequest> action;

      public AsyncEndpointRequest(Object whatToAdd, Action<AsyncEndpointRequest> action)
      {
         this.action = action;
         WhatToAdd = whatToAdd;
      }

      public bool IsCompleted
      {
         get
         {
            {
               return asyncResult.IsCompleted;
            }
         }
      }

      public void EndInvoke()
      {
         action.EndInvoke(asyncResult);
      }

      public void Start()
      {
         asyncResult = action.BeginInvoke(this, null, null);
      }
   }


   public abstract class AsyncEndpointRequestQueue
   {
      protected AsyncEndpointRequestQueue() { }
      public abstract AsyncEndpointRequest Add(AsyncEndpointRequest req, bool start);
      public abstract void EndInvokeAll();

      public static AsyncEndpointRequestQueue Create(int size)
      {
         return size > 1 ? (AsyncEndpointRequestQueue)new AsyncEndpointRequestQueueMulti(size) : new AsyncEndpointRequestQueueSingle();
      }
   }

   public class AsyncEndpointRequestQueueMulti : AsyncEndpointRequestQueue
   {
      private AsyncEndpointRequest[] q;
      private int killidx;

      public AsyncEndpointRequestQueueMulti(int size)
      {
         q = new AsyncEndpointRequest[size];
      }

      public override AsyncEndpointRequest Add(AsyncEndpointRequest req, bool start)
      {
         AsyncEndpointRequest elt;
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
         }

         elt = q[killidx];
         q[killidx] = req;
         killidx = (killidx + 1) % q.Length;
         elt.EndInvoke();

      OPTIONAL_START:
         if (start) req.Start();
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

   public class AsyncEndpointRequestQueueSingle : AsyncEndpointRequestQueue
   {
      private AsyncEndpointRequest q;

      public AsyncEndpointRequestQueueSingle()
      {
      }

      public override AsyncEndpointRequest Add(AsyncEndpointRequest req, bool start)
      {
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

         var elt = q;
         q = req;
         elt.EndInvoke();

      OPTIONAL_START:
         if (start) req.Start();
         return req;
      }

      public override void EndInvokeAll()
      {
         var elt = q;
         q = null;
         if (elt != null) elt.EndInvoke();
      }
   }
}
