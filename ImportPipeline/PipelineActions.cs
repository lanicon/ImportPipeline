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

namespace Bitmanager.ImportPipeline
{
   public abstract class PipelineAction : NamedItem
   {
      protected readonly Pipeline pipeline;
      protected Converter[] converters;
      protected IDataEndpoint endPoint;
      protected String endpointName, convertersName;
      protected KeyCheckMode checkMode;

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         this.pipeline = pipeline;
         endpointName = node.OptReadStr("@endpoint", pipeline.DefaultEndPoint);
         if (endpointName == null) node.ReadStr("@endpoint");
         endPoint = pipeline.ImportEngine.EndPoints.GetDataEndPoint(endpointName);

         convertersName = Converters.readConverters(node);
         converters = pipeline.ImportEngine.Converters.ToConverters(convertersName);

         checkMode = node.OptReadEnum<KeyCheckMode>("@check", 0);
         if (checkMode == KeyCheckMode.date) checkMode |= KeyCheckMode.key;
      }

      protected PipelineAction(String name) : base(name) { }  //Only needed for NOP action


      protected PipelineAction(PipelineAction template, String name, Regex regex)
         : base(name)
      {
         this.checkMode = template.checkMode;
         this.pipeline = template.pipeline;
         var engine = template.pipeline.ImportEngine;

         this.endpointName = optReplace (regex, name, template.endpointName);
         this.convertersName = optReplace (regex, name, template.convertersName); 

         this.endPoint = engine.EndPoints.GetDataEndPoint(endpointName);
         this.converters = engine.Converters.ToConverters(convertersName);
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
      protected Object Convert(Object obj)
      {
         if (converters == null) return obj;
         for (int i = 0; i < converters.Length; i++)
            obj = converters[i].Convert(obj);
         return obj;
      }

      public override string ToString()
      {
         return String.Format ("{0}: (key={1}, endpoint={2}, conv={3}", this.GetType().Name, Name, endpointName, convertersName);
      }

      public abstract Object HandleValue(PipelineContext ctx, String key, Object value);

      public static PipelineAction Create(Pipeline pipeline, XmlNode node)
      {
         if (node.SelectSingleNode("@add") != null) return new PipelineAddAction(pipeline, node);
         if (node.SelectSingleNode("@nop") != null) return new PipelineNopAction(pipeline, node);
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
         value = Convert(value);
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
}
