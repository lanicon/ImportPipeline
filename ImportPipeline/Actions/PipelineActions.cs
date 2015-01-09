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
   public enum _InternalActionType
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

      public static _InternalActionType GetActionType(XmlNode node)
      {
         _ActionType type = node.ReadEnum("@type", (_ActionType)0);
         switch (type)
         {
            case _ActionType.Emit: return _InternalActionType.Emit;
            case _ActionType.Field: return _InternalActionType.Field;
            case _ActionType.Add: return _InternalActionType.Add;
            case _ActionType.Nop: return _InternalActionType.Nop;
            case _ActionType.ErrorHandler: return _InternalActionType.ErrorHandler;
            case _ActionType.Except: return _InternalActionType.Except;
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
            case _InternalActionType.ErrorHandler: return new PipelineErrorAction(pipeline, node);
            case _InternalActionType.Except: return new PipelineExceptionAction(pipeline, node);
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
      protected String fromField;
      protected FieldFlags fieldFlags;
      protected String sep;

      public PipelineFieldAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         toVar = node.ReadStr("@tovar", null);
         fromVar = node.ReadStr("@fromvar", null);
         fromField = node.ReadStr("@fromfield", null);
         if (fromVar != null && fromField != null)
            throw new BMNodeException(node, "Cannot specify both fromvar and fromfield.");
 
         toField = node.ReadStr("@field", null);
         toFieldFromVar = node.ReadStr("@fieldfromvar", null);
         sep = node.ReadStr("@sep", null);
         fieldFlags = node.ReadEnum("@flags", sep==null ? FieldFlags.OverWrite : FieldFlags.Append);

         if (checkMode == 0 && toField == null && toVar == null && base.scriptName == null && toFieldFromVar == null)
            throw new BMNodeException(node, "At least one of 'field', 'toFieldFromVar', 'tovar', 'script', 'check'-attributes is mandatory.");
      }

      internal PipelineFieldAction(PipelineFieldAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.toField = optReplace(regex, name, template.toField);
         this.toVar = optReplace(regex, name, template.toVar);
         this.fromVar = optReplace(regex, name, template.fromVar);
         this.fromField = optReplace(regex, name, template.fromField);
         this.toFieldFromVar = optReplace(regex, name, template.toFieldFromVar);
         this.sep = template.sep;
         this.fieldFlags = template.fieldFlags;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         if (fromVar != null) value = ctx.Pipeline.GetVariable(fromVar);
         if (fromField != null) value = endPoint.GetField(fromField);

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
         return base.handleCheck(ctx, value);
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
         if (fromVar != null)
            sb.AppendFormat(", fromfield={0}", fromField);
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
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) { ctx.Skipped++; goto EXIT_RTN; }
         if (checkMode != 0)
         {
            Object res = base.handleCheck(ctx, value);
            if (res != null)
            {
               var existState = (ExistState)res;
               if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
               {
                  ctx.Skipped++;
                  goto EXIT_RTN;
               }
            }
         }
         ctx.IncrementAndLogAdd();
         endPoint.Add(ctx);
         endPoint.Clear();
         pipeline.ClearVariables();

         EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

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
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) { ctx.Skipped++; goto EXIT_RTN; }

         Exception err = value as Exception;
         if (err == null)
         {
            try
            {
               String msg = value==null ? "null" : value.ToString();
               throw new BMException(msg);
            }
            catch (Exception e)
            {
               err = e;
            }
         }

         ((IErrorEndpoint)Endpoint).SaveError (ctx, err);
         endPoint.Clear();
         pipeline.ClearVariables();

      EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

   public class PipelineNopAction : PipelineAction
   {
      public PipelineNopAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }
      public PipelineNopAction(String name) : base(name) { }

      internal PipelineNopAction(PipelineNopAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         return null;
      }
   }

   public class PipelineExceptionAction : PipelineAction
   {
      protected String msg;
      public PipelineExceptionAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         msg = node.ReadStr("@msg", "Exception requested by action.");
      }
      public PipelineExceptionAction(String name) : base(name) { }

      internal PipelineExceptionAction(PipelineExceptionAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.msg = optReplace(regex, name, template.msg);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         throw new Exception(msg);
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
         destination = node.ReadEnum("@destination", Destination.PipeLine);
         maxLevel = node.ReadInt("@maxlevel", 1);
         recField = node.ReadStr("@emitfield", null);
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
         return PostProcess (ctx, value);
      }

      protected override void _ToString(StringBuilder sb)
      {
         base._ToString(sb);
         sb.AppendFormat(", eventKey={0}, dest={1}, maxlevel={2}", eventKey, destination, maxLevel);
      }
   }
}
