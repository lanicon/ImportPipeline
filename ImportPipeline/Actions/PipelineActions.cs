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
      Remove = 16,
      CopyToEndpoint=17,
   }
   public abstract class PipelineAction : NamedItem
   {
      public delegate Object OldScriptDelegate(PipelineContext ctx, String key, Object value);
      public delegate Object ScriptDelegate(PipelineContext ctx, Object value);
      protected readonly Pipeline pipeline;
      protected readonly XmlNode node;
      protected static Logger logger;
      protected ValueSource valueSource;
      protected ScriptDelegate[] scriptDelegates;
      protected Converter[] converters;
      protected IDataEndpoint endPoint;
      protected readonly String postProcessors;
      protected readonly String endpointName, convertersName, scriptName;
      protected readonly String condExpr, valExpr;
      protected readonly String condExprFunc, valExprFunc;
      protected readonly String clrvarName;
      internal String[] VarsToClear;
      public readonly bool Debug;
      public readonly bool ConvertersFirst;
      public bool ConvertAndCallScriptNeeded;

      public IDataEndpoint Endpoint { get { return endPoint; } }
      public bool HasEndpointName { get { return endpointName != null; } }

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         this.pipeline = pipeline;
         this.node = node;
         if (logger == null) logger = pipeline.ImportEngine.DebugLog.Clone("action");
         Debug = node.ReadBool("@debug", false);
         ConvertersFirst = node.ReadBool("@convertersfirst", true);
         endpointName = node.ReadStr("@endpoint", null);
         postProcessors = node.ReadStr("@postprocessors", null);


         String src = node.ReadStr("@source", null);
         if (src != null) valueSource = ValueSource.Parse (src);

         scriptName = node.ReadStr("@script", null);
         valExpr = node.ReadStr("@valexpr", null);
         condExpr = node.ReadStr("@condexpr", null);
         if (valExpr != null)
         {
            valExprFunc = getGeneratedScriptName(pipeline, node, "expr");
            pipeline.ImportEngine.ScriptExpressions.AddExpression(valExprFunc, valExpr);
         }
         if (condExpr != null)
         {
            condExprFunc = getGeneratedScriptName(pipeline, node, "cond");
            pipeline.ImportEngine.ScriptExpressions.AddExpression(condExprFunc, condExpr);
         }

         clrvarName = node.ReadStr("@clrvar", null);
         VarsToClear = clrvarName.SplitStandard();
         
         convertersName = Converters.readConverters(node);
         if (convertersName == null) convertersName = pipeline.DefaultConverters;

         var x = this as PipelineForwardAction;
         if (x==null && node.ReadStr("@forward", null) != null)
            throw new BMNodeException (node, "[forward] attribute not supported. Use type='forward' instead."); 

         updateConvertAndCallScriptNeeded();
      }

      protected static String getGeneratedScriptName(Pipeline pipeline, XmlNode node, String what)
      {
         StringBuilder sb = new StringBuilder();
         sb.Append(pipeline.Name);
         sb.Append('_');
         sb.Append(node.Name);
         sb.Append('_');
         XmlNodeList list = node.ParentNode.SelectNodes(node.Name);
         for (int i = 0; i < list.Count; i++)
            if (list[i] == node) { sb.Append(i); break; }
         sb.Append('_');
         sb.Append(what);
         return sb.ToString();
      }

      protected PipelineAction(String name) : base(name) { }  //Only needed for NOP action


      protected PipelineAction(PipelineAction template, String name, Regex regex)
         : base(name)
      {
         this.Debug = template.Debug;
         this.ConvertersFirst = template.ConvertersFirst;
         this.pipeline = template.pipeline;
         this.node = template.node;
         this.endpointName = optReplace (regex, name, template.endpointName);
         this.postProcessors = optReplace(regex, name, template.postProcessors);
         this.convertersName = optReplace(regex, name, template.convertersName);
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
         this.valExpr = template.valExpr;
         this.valExprFunc = template.valExprFunc;
         this.condExpr = template.condExpr;
         this.condExprFunc = template.condExprFunc;
         updateConvertAndCallScriptNeeded();
      }

      protected void updateConvertAndCallScriptNeeded()
      {
         ConvertAndCallScriptNeeded = (valueSource != null || convertersName != null || scriptName != null || valExpr != null || condExpr != null);
      }

      public virtual void Start(PipelineContext ctx)
      {
         converters = ctx.ImportEngine.Converters.ToConverters(convertersName);
         endPoint = ctx.Pipeline.CreateOrGetDataEndpoint(ctx, endpointName, postProcessors);
         if (ConvertAndCallScriptNeeded)
         {
            var list = new List<ScriptDelegate>(4);
            if (valueSource != null) list.Add (valueSource.GetValue);

            if (ConvertersFirst) addConverters(list);

            if (condExpr != null)
               list.Add(pipeline.CreateScriptExprDelegate<ScriptDelegate>(condExprFunc, node));
            if (valExpr != null)
               list.Add(pipeline.CreateScriptExprDelegate<ScriptDelegate>(valExprFunc, node));
            if (scriptName != null)
               list.Add(pipeline.CreateScriptDelegate<ScriptDelegate>(scriptName, node));

            if (!ConvertersFirst) addConverters(list);
            scriptDelegates = list.ToArray();
         }
      }

      private void addConverters (List<ScriptDelegate> list)
      {
         if (converters!=null)
            foreach (var x in converters)
               list.Add(x.Convert);
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
         if (scriptDelegates != null)
         {
            foreach (var fn in scriptDelegates)
            {
               value = fn (ctx, value);
               if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return value;
            }
         }
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
         if (valExpr != null) b.AppendFormat(", valexpr={0}", valExpr);
         if (condExpr != null) b.AppendFormat(", condexpr={0}", condExpr);
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
            case _ActionType.Remove: return new PipelineRemoveAction(pipeline, node);
            case _ActionType.CopyToEndpoint: return new PipelineCopyToEndpointAction(pipeline, node);
         }
         act.ThrowUnexpected();
         return null; //Keep compiler happy
      }

   }






}
