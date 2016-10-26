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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Xml;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class StreamDirectoryEnumerator : IEnumerator<IStreamProvider>
   {
      protected Logger logger;
      protected readonly StreamDirectory parent;
      protected readonly PipelineContext ctx;
      protected readonly StackDirElt topStackElement;
      protected IStreamProvider cur;
      private List<StackElt> stack;

      protected StreamDirectoryEnumerator(PipelineContext ctx, StreamDirectory parent, StackDirElt topElt)
      {
         this.ctx = ctx;
         this.parent = parent;
         stack = new List<StackElt>();
         stack.Add(topStackElement = topElt);
         logger = ctx.ImportLog.Clone (parent.GetType().Name);
      }

      public StreamDirectoryEnumerator(PipelineContext ctx, StreamDirectory parent, IEnumerator<object> e)
         : this(ctx, parent, new StackDirElt(parent, e))
      {
      }

      public StreamDirectoryEnumerator(PipelineContext ctx, StreamDirectory parent, IEnumerable<object> e)
         : this(ctx, parent, new StackDirElt(parent, e.GetEnumerator()))
      {
      }

      public virtual IStreamProvider Current
      {
         get { return cur; }
      }

      public virtual void Dispose()
      {
      }

      object System.Collections.IEnumerator.Current
      {
         get { return cur; }
      }

      public bool MoveNext()
      {
         const bool debug=false;
         if (debug) logger.Log("MoveNext()");
         while (true)
         {
            if (debug) logger.Log("-- stackkount={0}", stack.Count);
            if (stack.Count == 0) return false;
            Object next = stack[0].GetNext();
            if (debug) logger.Log("-- next={0}", next);
            if (next == null)
            {
               if (debug) logger.Log("-- parent.forcedNext={0}", parent.forcedNext);
               if (parent.forcedNext != null)
               {
                  int pos = 1;
                  while (parent.forcedNext.Count > 0)
                  {
                     Object queuedElt = parent.forcedNext.Dequeue();
                     if (debug) logger.Log("-- queuedElt={0}", queuedElt);
                     var tmp = queuedElt as IStreamProvider;
                     var stackElt = (tmp != null) ? (StackElt)new StackProviderElt(tmp) : new StackDirElt(ctx, (StreamDirectory)queuedElt);

                     if (debug) logger.Log("-- insertat={0}", pos);
                     stack.Insert(pos, stackElt);
                     pos++;
                  }
               }
               if (debug) logger.Log("-- remove={0}", 0);
               stack.RemoveAt(0);
               continue;
            }

            this.cur = next as IStreamProvider;
            if (debug) logger.Log("-- nextasprov={0}", this.cur);

            if (this.cur != null) return true;

            if (debug) logger.Log("-- insertdir={0}", next);
            stack.Insert(0, new StackDirElt(ctx, (StreamDirectory)next));
         }
      }

      public void Reset()
      {
         topStackElement.Enumerator.Reset();
         stack.Clear();
         stack.Add(topStackElement);
      }

      protected abstract class StackElt
      {
         public abstract Object GetNext();
      }

      protected class StackDirElt : StackElt
      {
         public readonly StreamDirectory Directory;
         public readonly System.Collections.IEnumerator Enumerator;

         public StackDirElt(PipelineContext ctx, StreamDirectory d)
         {
            Directory = d;
            Enumerator = d.GetProviders(ctx).GetEnumerator();
         }
         public StackDirElt(StreamDirectory d, IEnumerator<Object> e)
         {
            Directory = d;
            Enumerator = e;
         }
         public override Object GetNext()
         {
            return Enumerator.MoveNext() ? Enumerator.Current : null;
         }
      }

      protected class StackProviderElt : StackElt
      {
         private IStreamProvider provider;
         public readonly IEnumerator<Object> Enumerator;

         public StackProviderElt(IStreamProvider provider)
         {
            this.provider = provider;
         }
         public override Object GetNext()
         {
            var ret = provider;
            provider = null;
            return ret;
         }
      }
   }
}
