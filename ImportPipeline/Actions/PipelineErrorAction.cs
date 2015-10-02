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
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline
{
   public class PipelineErrorAction : PipelineAction
   {
      public PipelineErrorAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      internal PipelineErrorAction(PipelineAddAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override void Start(PipelineContext ctx)
      {
         base.Start(ctx);
         IErrorEndpoint ep = Endpoint as IErrorEndpoint;
         if (ep == null) throw new BMException("Endpoint does not support IErrorEndpoint. Action={0}", this);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) goto EXIT_RTN;

         Exception err = value as Exception;
         if (err == null)
         {
            try
            {
               String msg = value == null ? "null" : value.ToString();
               throw new BMException(msg);
            }
            catch (Exception e)
            {
               err = e;
            }
         }

         try
         {
            ((IErrorEndpoint)Endpoint).SaveError(ctx, err);
         }
         catch (Exception e)
         {
            const String msg = "Failed to write exception to error endpoint.";
            ctx.ErrorLog.Log(e, msg);
            ctx.ErrorLog.Log(e, "Original error");
            throw new BMException(e, msg);
         }

         endPoint.Clear();
         pipeline.ClearVariables();

      EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

}
