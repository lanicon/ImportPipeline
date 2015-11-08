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

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Wraps an old type script and behaves like a new script call
   /// </summary>
   public class ScriptOldWrapper
   {
      PipelineAction.OldScriptDelegate oldDelegate;
      public ScriptOldWrapper (PipelineAction.OldScriptDelegate fn)
      {
         oldDelegate = fn;
      }

      public Object CallScript (PipelineContext ctx, Object value)
      {
         return oldDelegate(ctx, ctx.Action.Name, value);
      }

      public PipelineAction.ScriptDelegate CreateDelegate ()
      {
         return CallScript;
      }


   }
}
