using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;

namespace Bitmanager.ImportPipeline
{
   public class PipelineEndPoint
   {
      Logger addLogger = Logs.CreateLogger("pipelineAdder", "pipelineAdder");
      public readonly String Name;
      public PipelineEndPoint(String name)
      {
         Name = name;
         Clear();
      }

      JObject accumulator;

      public void Clear()
      {
         accumulator = new JObject();
      }

      public void SetField(String fld, Object value)
      {
         addLogger.Log("-- setfield {0}: '{1}'", fld, value);
         accumulator.WriteToken(fld, value); 
      }

      public void Add(String[] toWhat)
      {
         addLogger.Log(accumulator.ToString (Newtonsoft.Json.Formatting.Indented));
      }
   }
   public abstract class PipelineAction : NamedItem
   {
      protected Converter[] converters;
      protected IDataEndpoint endPoint;
      protected String endpointName, convertersName;

      public PipelineAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         String endpointName = node.OptReadStr("@endpoint", pipeline.DefaultEndPoint);
         if (endpointName == null) node.ReadStr("@endpoint");
         String convertersName;
         endPoint = pipeline.ImportEngine.EndPoints.GetDataEndPoint(ReadParameters(pipeline, node, out convertersName));
         converters = pipeline.ImportEngine.Converters.ToConverters(convertersName);
      }

      protected PipelineAction(Pipeline pipeline, String name, String endpointName, String converters)
         : base(name)
      {
         this.endpointName = endpointName;
         this.convertersName = converters;
         this.endPoint = pipeline.ImportEngine.EndPoints.GetDataEndPoint(endpointName);
         this.converters = pipeline.ImportEngine.Converters.ToConverters(converters);
      }
      protected PipelineAction(String name)
         : base(name)
      {
      }

      public static String ReadParameters(Pipeline pipeline, XmlNode node, out String converters)
      {
         converters = node.OptReadStr("@converters", null);
         String endpointName = node.OptReadStr("@endpoint", pipeline.DefaultEndPoint);
         return (endpointName != null) ? endpointName : node.ReadStr("@endpoint");
      }

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
         if (node.SelectSingleNode("@field") != null) return new PipelineFieldAction(pipeline, node);
         if (node.SelectSingleNode("@add") != null) return new PipelineAddAction(pipeline, node);
         if (node.SelectSingleNode("@nop") != null) return new PipelineNopAction(node);
         throw new BMNodeException(node, "Don't know how to create an action for this node.");
      }
   }

   public class PipelineFieldAction : PipelineAction
   {
      protected String toField;

      public PipelineFieldAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         toField = node.ReadStr("@field");
      }

      //If you change something in the constructor, please check PipelineFieldTemplate as well
      public PipelineFieldAction(Pipeline pipeline, String name, String endpointName, String converters, String field)
         : base(pipeline, name, endpointName, converters)
      {
         this.toField = field;
      }


      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = Convert(value);
         endPoint.SetField (toField, value);
         return null;
      }
      public override string ToString()
      {
         return base.ToString() + ", field=" + toField + ")";
      }
   }



   public class PipelineAddAction : PipelineAction
   {
      public PipelineAddAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      //If you change something in the constructor, please check PipelineAddTemplate as well
      public PipelineAddAction(Pipeline pipeline, String name, String endpointName, String converters)
         : base(pipeline, name, endpointName, converters)
      {
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         endPoint.Add();
         endPoint.Clear();
         return null;
      }

      public override string ToString()
      {
         return base.ToString() + ")";
      }

   }

   public class PipelineNopAction : PipelineAction
   {
      public PipelineNopAction(XmlNode node)
         : base (node.ReadStr ("@key"))
      {
      }

      //If you change something in the constructor, please check PipelineAddTemplate as well
      public PipelineNopAction(String name)
         : base(name)
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
