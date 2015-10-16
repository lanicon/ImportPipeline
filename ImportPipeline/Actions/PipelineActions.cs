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
using Bitmanager.Elastic;
using System.Reflection;

namespace Bitmanager.ImportPipeline
{
   public enum _ActionType
   {
      Nop = 1,
      OrgField = 2,
      Add = 3,
      Emit = 4,
      ErrorHandler = 5,
      Except = 6,
      Clear = 7,
      Clr = 7,
      Delete = 8,
      Del = 8,
      Category = 9,
      Cat = 9,
      Cond = 10,
      Condition = 10,
      CheckExist = 11,
      Forward = 12,
      Split = 13,
      EmitVars = 14,
      Field = 15,
   }
   public abstract class PipelineAction : NamedItem
   {
      public delegate Object ScriptDelegate(PipelineContext ctx, String key, Object value);
      protected readonly Pipeline pipeline;
      protected readonly XmlNode node;
      protected static Logger logger;
      protected ValueSource valueSource;
      protected ScriptDelegate scriptDelegate;
      protected Converter[] converters;
      protected IDataEndpoint endPoint;
      protected String endpointName, convertersName, scriptName;
      protected String clrvarName;
      internal String[] VarsToClear;
      public readonly bool Debug;

      public IDataEndpoint Endpoint { get { return endPoint; } }
      public bool HasEndpointName { get { return endpointName != null; } }

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         this.pipeline = pipeline;
         this.node = node;
         if (logger == null) logger = pipeline.ImportEngine.DebugLog.Clone("action");
         Debug = node.ReadBool("@debug", false);
         endpointName = node.ReadStr("@endpoint", null);

         String src = node.ReadStr("@source", null);
         if (src != null) valueSource = ValueSource.Parse (src);

         scriptName = node.ReadStr("@script", null);

         clrvarName = node.ReadStr("@clrvar", null);
         VarsToClear = clrvarName.SplitStandard();
         
         convertersName = Converters.readConverters(node);
         if (convertersName == null) convertersName = pipeline.DefaultConverters;

         var x = this as PipelineForwardAction;
         if (x==null && node.ReadStr("@forward", null) != null)
            throw new BMNodeException (node, "[forward] attribute not supported. Use type='forward' instead."); 
      }

      protected PipelineAction(String name) : base(name) { }  //Only needed for NOP action


      protected PipelineAction(PipelineAction template, String name, Regex regex)
         : base(name)
      {
         this.Debug = template.Debug;
         this.pipeline = template.pipeline;
         this.node = template.node;
         this.endpointName = optReplace (regex, name, template.endpointName);
         this.convertersName = optReplace (regex, name, template.convertersName);
         this.scriptName = optReplace(regex, name, template.scriptName);
         this.clrvarName = optReplace(regex, name, template.clrvarName);
         if (this.clrvarName == template.clrvarName)
            this.VarsToClear = template.VarsToClear;
         else
            this.VarsToClear = this.clrvarName.SplitStandard();

         if (template.valueSource != null)
         {
            String src = optReplace(regex, name, template.valueSource.Input);
            if (src == template.valueSource.Input)
               valueSource = template.valueSource;
            else
               valueSource = ValueSource.Parse(src);
         }
      }

      public virtual void Start(PipelineContext ctx)
      {
         converters = ctx.ImportEngine.Converters.ToConverters(convertersName);
         endPoint = ctx.Pipeline.GetDataEndpoint(ctx, endpointName);
         if (scriptName != null)
            scriptDelegate = pipeline.CreateScriptDelegate<ScriptDelegate>(scriptName, node);
      }

      protected static String optReplace(Regex regex, String arg, String repl)
      {
         if (repl == null) return null;
         if (repl.IndexOf('$') < 0) return repl;
         return regex.Replace(arg, repl);
      }

      /// <summary>
      /// Optional converts the value according to the supplied converters
      /// </summary>
      protected Object ConvertAndCallScript(PipelineContext ctx, String key, Object value)
      {
         if (valueSource != null) value = valueSource.GetValue(ctx, value);
         if (scriptDelegate != null)
         {
            value = scriptDelegate(ctx, key, value);
            if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return value;
         }

         if (converters == null) return value;
         for (int i = 0; i < converters.Length; i++)
            value = converters[i].Convert(ctx, value);
         return value;
      }

      public override string ToString()
      {
         StringBuilder b = new StringBuilder();
         _ToString(b);
         b.Append(')');
         return b.ToString();
      }
      protected virtual void _ToString(StringBuilder b)
      {
         b.AppendFormat("{0}: (key={1}", this.GetType().Name, Name);
         if (endpointName != null) b.AppendFormat(", endpoint={0}", endpointName);
         if (convertersName != null) b.AppendFormat(", conv={0}", convertersName);
         if (scriptName != null) b.AppendFormat(", script={0}", scriptName);
         if (clrvarName != null) b.AppendFormat(", clrvar={0}", clrvarName);
         if (valueSource != null) b.AppendFormat(", source={0}", valueSource);
      }

      public abstract Object HandleValue(PipelineContext ctx, String key, Object value);

      public static _ActionType GetActionType(XmlNode node)
      {
         _ActionType type = node.ReadEnum("@type", (_ActionType)0);
         if (type != 0) return type;

         if (node.SelectSingleNode("@add") != null) return _ActionType.Add;
         if (node.SelectSingleNode("@nop") != null) return _ActionType.Nop;
         if (node.SelectSingleNode("@emitexisting") != null) return _ActionType.Emit;
         return _ActionType.Field;
      }

      public static PipelineAction Create(Pipeline pipeline, XmlNode node)
      {
         _ActionType act = GetActionType (node); 
         switch (act)
         {
            case _ActionType.Add: return new PipelineAddAction(pipeline, node);
            case _ActionType.Clr: return new PipelineClearAction(pipeline, node);
            case _ActionType.Nop: return new PipelineNopAction(pipeline, node);
            case _ActionType.OrgField: return new PipelineFieldAction(pipeline, node);
            case _ActionType.Field: return new PipelineFieldAction2(pipeline, node);
            case _ActionType.Emit: return new PipelineEmitAction(pipeline, node);
            case _ActionType.ErrorHandler: return new PipelineErrorAction(pipeline, node);
            case _ActionType.Except: return new PipelineExceptionAction(pipeline, node);
            case _ActionType.Del: return new PipelineDeleteAction(pipeline, node);
            case _ActionType.Cat: return new PipelineCategorieAction(pipeline, node);
            case _ActionType.Cond: return new PipelineConditionAction(pipeline, node);
            case _ActionType.CheckExist: return new PipelineCheckExistAction(pipeline, node);
            case _ActionType.Forward: return new PipelineForwardAction(pipeline, node);
            case _ActionType.Split: return new PipelineSplitAction(pipeline, node);
            case _ActionType.EmitVars: return new PipelineEmitVarsAction(pipeline, node);
         }
         act.ThrowUnexpected();
         return null; //Keep compiler happy
      }

   }






}
