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
   }
   public enum _InternalActionType
   {
      Nop = 1,
      Field = 2,
      Add = 3,
      Emit = 4,
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
      protected String clrvarName;
      internal String[] VarsToClear;
      protected KeyCheckMode checkMode;

      public IDataEndpoint Endpoint { get { return endPoint; } }

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         this.pipeline = pipeline;
         this.node = node;
         if (logger == null) logger = pipeline.ImportEngine.DebugLog.Clone("action");
         endpointName = node.OptReadStr("@endpoint", pipeline.DefaultEndpoint);
         if (endpointName == null) node.ReadStr("@endpoint");

         scriptName = node.OptReadStr("@script", null);

         clrvarName = node.OptReadStr("@clrvar", null);
         VarsToClear = clrvarName.SplitStandard();
         
         convertersName = Converters.readConverters(node);
         if (convertersName == null) convertersName = pipeline.DefaultConverters;

         checkMode = node.OptReadEnum<KeyCheckMode>("@check", 0);
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
         this.scriptName = optReplace (regex, name, template.scriptName);
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
      protected Object handleCheck (PipelineContext ctx)
      {
         if (checkMode != 0)
         {
            Object k = ctx.Pipeline.GetVariable("key");
            if (k != null)
            {
               Object date = null;
               if ((checkMode & KeyCheckMode.date) != 0)
                  date = ctx.Pipeline.GetVariable("date");
               return endPoint.Exists(ctx, (String)k, (DateTime?)date);
            }
         }
         return null;
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

      public static _InternalActionType GetActionType(XmlNode node)
      {
         _ActionType type = node.OptReadEnum("@type", (_ActionType)0);
         switch (type)
         {
            case _ActionType.Emit: return _InternalActionType.Emit;
            case _ActionType.Field: return _InternalActionType.Field;
            case _ActionType.Add: return _InternalActionType.Add;
            case _ActionType.Nop: return _InternalActionType.Nop;
         }
         if (node.SelectSingleNode("@add") != null) return _InternalActionType.Add;
         if (node.SelectSingleNode("@nop") != null) return _InternalActionType.Nop;
         if (node.SelectSingleNode("@emitexisting") != null) return _InternalActionType.Emit;
         return _InternalActionType.Field;
      }

      public static PipelineAction Create(Pipeline pipeline, XmlNode node)
      {
         _InternalActionType act = GetActionType (node); 
         switch (act)
         {
            case _InternalActionType.Add: return new PipelineAddAction(pipeline, node);
            case _InternalActionType.Nop: return new PipelineNopAction(pipeline, node);
            case _InternalActionType.Field: return new PipelineFieldAction(pipeline, node);
            case _InternalActionType.Emit: return new PipelineEmitAction(pipeline, node);
         }
         throw new Exception ("Unexpected _InternalActionType: " + act);
      }
   }

   public class PipelineFieldAction : PipelineAction
   {
      protected String toField;
      protected String toFieldFromVar;
      protected String toVar;
      protected String fromVar;
      protected FieldFlags fieldFlags;
      protected String sep;

      public PipelineFieldAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         toVar = node.OptReadStr("@tovar", null);
         fromVar = node.OptReadStr("@fromvar", null);
         toField = node.OptReadStr("@field", null);
         toFieldFromVar = node.OptReadStr("@fieldfromvar", null);
         sep = node.OptReadStr("@sep", null);
         fieldFlags = node.OptReadEnum("@flags", sep==null ? FieldFlags.OverWrite : FieldFlags.Append);

         if (checkMode == 0 && toField == null && toVar == null && base.scriptName == null && toFieldFromVar == null)
            throw new BMNodeException(node, "At least one of 'field', 'toFieldFromVar', 'tovar', 'script', 'check'-attributes is mandatory.");
      }

      internal PipelineFieldAction(PipelineFieldAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.toField = optReplace(regex, name, template.toField);
         this.toVar = optReplace(regex, name, template.toVar);
         this.fromVar = optReplace(regex, name, template.fromVar);
         this.toFieldFromVar = optReplace(regex, name, template.toFieldFromVar);
         this.sep = template.sep;
         this.fieldFlags = template.fieldFlags;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         if (fromVar != null) value = ctx.Pipeline.GetVariable(fromVar);

         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;

         if (toField != null)
            endPoint.SetField (toField, value, fieldFlags, sep);
         else
            if (toFieldFromVar != null)
            {
               String fn = ctx.Pipeline.GetVariableStr(toFieldFromVar);
               if (fn != null) endPoint.SetField(fn, value, fieldFlags, sep);
            }
         if (toVar != null) ctx.Pipeline.SetVariable(toVar, value);
         return base.handleCheck(ctx);
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         if (toField != null)
            sb.AppendFormat (", field={0}", toField);
         else if (toFieldFromVar != null)
            sb.AppendFormat(", fieldfromvar={0}", toFieldFromVar);
         if (fromVar != null)
            sb.AppendFormat(", fromvar={0}", fromVar);
         if (toVar != null)
            sb.AppendFormat(", tovar={0}", toVar);
      }
   }

   public enum KeyCheckMode
   {
      key=1, date=2
   }
   public class PipelineAddAction : PipelineAction
   {
      public PipelineAddAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      internal PipelineAddAction (PipelineAddAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;
         if (checkMode != 0)
         {
            Object res = base.handleCheck(ctx);
            if (res != null)
            {
               var existState = (ExistState)res;
               if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
               {
                  ctx.Skipped++;
                  return null;
               }
            }
         }
         endPoint.Add(ctx);
         endPoint.Clear();
         ctx.CountAndLogAdd();
         pipeline.ClearVariables();
         return null;
      }
   }

   public class PipelineNopAction : PipelineAction
   {
      public PipelineNopAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }
      public PipelineNopAction(String name): base(name){}

      internal PipelineNopAction(PipelineNopAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override Object  HandleValue(PipelineContext ctx, String key, Object value)
      {
         return null;
      }
   }

   public class PipelineEmitAction : PipelineAction
   {
      enum Destination { PipeLine = 1, Datasource = 2 };

      private String eventKey;
      private String recField;
      private int maxLevel;
      private Destination destination; 
      public PipelineEmitAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         eventKey = node.ReadStr("@emitexisting");
         destination = node.OptReadEnum("@destination", Destination.PipeLine);
         maxLevel = node.OptReadInt("@maxlevel", 1);
         recField = node.OptReadStr("@emitfield", null);
      }

      internal PipelineEmitAction(PipelineEmitAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.eventKey = optReplace(regex, name, template.eventKey);
         this.recField = optReplace(regex, name, template.recField);
         this.destination = template.destination;
         this.maxLevel = template.maxLevel;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         IDatasourceSink sink = ctx.Pipeline;
         if (destination == Destination.Datasource) sink = (IDatasourceSink)ctx.DatasourceAdmin.Datasource;
         String reckey = (String)ctx.Pipeline.GetVariable ("key");
         if (reckey==null) return null;

         this.endPoint.EmitRecord(ctx, reckey, recField, sink, eventKey, maxLevel);
         return null;
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", eventKey={0}, dest={1}, maxlevel={2}", eventKey, destination, maxLevel);
      }
   }
}
