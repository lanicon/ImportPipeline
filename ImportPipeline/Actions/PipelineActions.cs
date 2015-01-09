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
      Field = 2,
      Add = 3,
      Emit = 4,
      ErrorHandler = 5,
      Except = 6
   }
   public delegate Object ScriptDelegate(PipelineContext ctx, String key, Object value); 
   public abstract class PipelineAction : NamedItem
   {
      protected readonly Pipeline pipeline;
      protected readonly XmlNode node;
      protected static Logger logger;
      protected ScriptDelegate scriptDelegate;
      protected Converter[] converters;
      protected IDataEndpoint endPoint;
      protected String endpointName, convertersName, scriptName;
      protected String forwardTo;
      protected String clrvarName;
      internal String[] VarsToClear;
      protected KeyCheckMode checkMode;

      public IDataEndpoint Endpoint { get { return endPoint; } }
      public bool HasEndpointName { get { return endpointName != null; } }

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         this.pipeline = pipeline;
         this.node = node;
         if (logger == null) logger = pipeline.ImportEngine.DebugLog.Clone("action");
         endpointName = node.ReadStr("@endpoint", null);

         scriptName = node.ReadStr("@script", null);
         forwardTo = node.ReadStr("@forward", null);

         clrvarName = node.ReadStr("@clrvar", null);
         VarsToClear = clrvarName.SplitStandard();
         
         convertersName = Converters.readConverters(node);
         if (convertersName == null) convertersName = pipeline.DefaultConverters;

         checkMode = node.ReadEnum<KeyCheckMode>("@check", 0);
         if (checkMode == KeyCheckMode.date) checkMode |= KeyCheckMode.key;
      }

      protected PipelineAction(String name) : base(name) { }  //Only needed for NOP action


      protected PipelineAction(PipelineAction template, String name, Regex regex)
         : base(name)
      {
         this.checkMode = template.checkMode;
         this.pipeline = template.pipeline;
         this.node = template.node;
         this.endpointName = optReplace (regex, name, template.endpointName);
         this.convertersName = optReplace (regex, name, template.convertersName);
         this.scriptName = optReplace(regex, name, template.scriptName);
         this.forwardTo = optReplace(regex, name, template.forwardTo);
         this.clrvarName = optReplace(regex, name, template.clrvarName);
         if (this.clrvarName == template.clrvarName)
            this.VarsToClear = template.VarsToClear;
         else
            this.VarsToClear = this.clrvarName.SplitStandard();
      }

      public virtual void Start(PipelineContext ctx)
      {
         converters = ctx.ImportEngine.Converters.ToConverters(convertersName);
         endPoint = ctx.Pipeline.GetDataEndpoint(ctx, endpointName);
         if (scriptName != null)
         {
            Object scriptObj = pipeline.ScriptObject;
            if (scriptObj == null) throw new BMNodeException(node, "Script without a script on the pipeline is not allowed!");
            Type t = scriptObj.GetType();

            MethodInfo mi = t.GetMethod(scriptName, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Instance);
            if (mi == null) throw new BMNodeException(node, "Cannot find method {0} in class {1}.", scriptName, t.FullName);
            scriptDelegate = (ScriptDelegate)Delegate.CreateDelegate(typeof(ScriptDelegate), scriptObj, mi);
            logger.Log("-- Created delegate={0}", scriptDelegate);
         }
      }

      protected static String optReplace(Regex regex, String arg, String repl)
      {
         if (repl == null) return null;
         if (repl.IndexOf('$') < 0) return repl;
         return regex.Replace(arg, repl);
      }

      /// <summary>
      /// Handles the key/date check if checkMode <> 0
      /// </summary>
      /// <returns>null or an ExistState enumeration</returns>
      protected Object handleCheck (PipelineContext ctx, Object value)
      {
         if (checkMode != 0)
         {
            Object k = ctx.Pipeline.GetVariable("key");
            if (k != null)
            {
               Object date = null;
               if ((checkMode & KeyCheckMode.date) != 0)
                  date = ctx.Pipeline.GetVariable("date");
               ExistState es = endPoint.Exists(ctx, (String)k, (DateTime?)date);
               PostProcess(ctx, value);
               return es;
            }
         }
         return PostProcess(ctx, value);
      }

      /// <summary>
      /// Optional converts the value according to the supplied converters
      /// </summary>
      protected Object ConvertAndCallScript(PipelineContext ctx, String key, Object value)
      {
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


      /// <summary>
      /// Optional forward the value to another action
      /// </summary>
      protected Object PostProcess(PipelineContext ctx, Object value)
      {
         return (forwardTo == null) ? null : ctx.Pipeline.HandleValue(ctx, forwardTo, value);
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
         if (checkMode != 0) b.AppendFormat(", check={0}", checkMode);
         if (clrvarName != null) b.AppendFormat(", clrvar={0}", clrvarName);
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
            case _ActionType.Nop: return new PipelineNopAction(pipeline, node);
            case _ActionType.Field: return new PipelineFieldAction(pipeline, node);
            case _ActionType.Emit: return new PipelineEmitAction(pipeline, node);
            case _ActionType.ErrorHandler: return new PipelineErrorAction(pipeline, node);
            case _ActionType.Except: return new PipelineExceptionAction(pipeline, node);
         }
         act.ThrowUnexpected();
         return null; //Keep compiler happy
      }

   }






}
