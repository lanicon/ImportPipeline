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
   //public enum ScriptReturnFlagsV
   public delegate Object ScriptDelegate (PipelineContext ctx, String key, Object value); 
   public abstract class PipelineAction : NamedItem
   {
      protected readonly Pipeline pipeline;
      protected readonly XmlNode node;
      protected static Logger logger;
      protected ScriptDelegate scriptDelegate;
      protected Converter[] converters;
      protected IDataEndpoint endPoint;
      protected String endpointName, convertersName, scriptName;
      protected KeyCheckMode checkMode;

      public IDataEndpoint EndPoint { get { return endPoint; } }

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         this.pipeline = pipeline;
         this.node = node;
         if (logger == null) logger = pipeline.ImportEngine.DebugLog.Clone("action");
         endpointName = node.OptReadStr("@endpoint", pipeline.DefaultEndPoint);
         if (endpointName == null) node.ReadStr("@endpoint");

         scriptName = node.OptReadStr("@script", null);
         convertersName = Converters.readConverters(node);
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
      }

      public virtual void Start(PipelineContext ctx)
      {
         converters = ctx.ImportEngine.Converters.ToConverters(convertersName);
         endPoint = ctx.Pipeline.GetDataEndPoint(ctx, endpointName);
         logger.Log("Script=" + scriptName);
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
            value = converters[i].Convert(value);
         return value;
      }

      public override string ToString()
      {
         StringBuilder b = new StringBuilder();
         b.AppendFormat("{0}: (key={1}", this.GetType().Name, Name);
         if (endpointName != null) b.AppendFormat(", endpoint={0}", endpointName);
         if (convertersName != null) b.AppendFormat(", conv={0}", convertersName);
         if (scriptName != null) b.AppendFormat(", script={0}", scriptName);
         if (checkMode != 0) b.AppendFormat(", check={0}", checkMode);
         return b.ToString();
      }

      public abstract Object HandleValue(PipelineContext ctx, String key, Object value);

      public static PipelineAction Create(Pipeline pipeline, XmlNode node)
      {
         if (node.SelectSingleNode("@add") != null) return new PipelineAddAction(pipeline, node);
         if (node.SelectSingleNode("@nop") != null) return new PipelineNopAction(pipeline, node);
         if (node.SelectSingleNode("@emitexisting") != null) return new PipelineEmitAction(pipeline, node);
         return new PipelineFieldAction(pipeline, node);
      }
   }

   public class PipelineFieldAction : PipelineAction
   {
      protected String toField;
      protected String varName;

      public PipelineFieldAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         varName = node.OptReadStr("@tovar", null);
         toField = node.OptReadStr("@field", null);

         if (checkMode == 0 && toField == null && varName == null)
            throw new BMNodeException(node, "At least one of 'field', 'tovar', 'check'-attributes is mandatory.");
      }

      internal PipelineFieldAction(PipelineFieldAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         this.toField = optReplace(regex, name, template.toField);
         this.varName = optReplace(regex, name, template.varName);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) return null;

         if (toField != null) endPoint.SetField (toField, value);
         if (varName != null) ctx.Pipeline.SetVariable(varName, value);
         return base.handleCheck(ctx);
      }

      public override string ToString()
      {
         return base.ToString() + String.Format (", field={0}, var={1})", toField, varName);
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
         ctx.Added++;
         ctx.Pipeline.ClearVariables();
         return null;
      }

      public override string ToString()
      {
         return base.ToString() + ")";
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

      public override string ToString()
      {
         return base.ToString() + ")";
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

      public override string ToString()
      {
         return String.Format("{0} eventKey={1}, dest={2}, maxlevel={3})", base.ToString(), eventKey, destination, maxLevel);
      }

   }
}
