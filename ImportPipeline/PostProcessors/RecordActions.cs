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
using Bitmanager.Elastic;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public interface IRecordAction
   {
      IRecordAction Clone(PipelineContext ctx);
      void ProcessRecord(PipelineContext ctx, JObject rec);
   }

   public abstract class RecordActionBase : IRecordAction
   {
      public virtual IRecordAction Clone(PipelineContext ctx)
      {
         return this;
      }
      public abstract void ProcessRecord(PipelineContext ctx, JObject rec);
   }



   public class RecordActions
   {
      List<IRecordAction> actions;
      public RecordActions(IPostProcessor processor, XmlNode node)
      {
         XmlNodeList nodes = node.SelectNodes("action");
         actions = new List<IRecordAction>(nodes.Count);
         foreach (XmlNode child in nodes)
         {
            actions.Add(CreateAction(processor, child));
         }
      }

      public static IRecordAction CreateAction(IPostProcessor processor, XmlNode node)
      {
         String type = node.ReadStr("@type");
         if ("add".Equals(type, StringComparison.OrdinalIgnoreCase)) return new RecordPruneAction(processor, node);
         throw new BMException("Unrecognized type [{0}] for a post process record-action.", type);
         //TODO make this more generic
      }

      public void ProcessRecord(PipelineContext ctx, JObject record)
      {
         foreach (var a in actions)
            a.ProcessRecord(ctx, record);
      }

   }

   public class RecordPruneAction : RecordActionBase
   {
      String[] fields;
      public RecordPruneAction(IPostProcessor processor, XmlNode node)
      {
         fields = node.ReadStr("@field").SplitStandard();
      }

      public override void ProcessRecord(PipelineContext ctx, JObject record)
      {
         foreach (var f in fields)
            record.Remove(f);
      }
   }

   public class RecordScriptAction : RecordActionBase
   {
      public delegate void ScriptDelegate(PipelineContext ctx, JObject record);
      public readonly String ScriptName;
      protected ScriptDelegate scriptDelegate;

      public RecordScriptAction(IPostProcessor processor, XmlNode node)
      {
         ScriptName = node.ReadStr("@script");
      }
      protected RecordScriptAction(PipelineContext ctx, RecordScriptAction other)
      {
         ScriptName = other.ScriptName;
         scriptDelegate = ctx.Pipeline.CreateScriptDelegate<ScriptDelegate>(ScriptName);
      }
      public override IRecordAction Clone(PipelineContext ctx)
      {
         return new RecordScriptAction(ctx, this);
      }


      public override void ProcessRecord(PipelineContext ctx, JObject record)
      {
         scriptDelegate (ctx, record);
      }
   }
}
