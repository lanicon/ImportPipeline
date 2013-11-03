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
   public class PipelineContext
   {
      Logger addLogger = Logs.CreateLogger("pipelineAdder", "pipelineAdder");
      JObject accumulator;
      public PipelineContext()
      {
         Clear();
      }

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

   public abstract class PipelineDataAction : NamedItem
   {
      protected Converter[] converters;
      protected IDataEndpoint endPoint;


      public PipelineDataAction(Pipeline pipeline, XmlNode node)
         : base(node, "@key")
      {
         String endpointName = node.OptReadStr("@endpoint", pipeline.DefaultEndPoint);
         if (endpointName == null) node.ReadStr("@endpoint");

         endPoint = pipeline.ImportEngine.EndPoints.GetDataEndPoint(endpointName);
         converters = pipeline.ImportEngine.Converters.ToConverters(node.OptReadStr("@converters", null));
      }

      protected Object Convert(Object obj)
      {
         if (converters == null) return obj;
         for (int i = 0; i < converters.Length; i++)
            obj = converters[i].Convert(obj);
         return obj;
      }

      public abstract void HandleValue(PipelineContext ctx, String key, Object value);

      public static PipelineDataAction Create(Pipeline pipeline, XmlNode node)
      {
         if (node.SelectSingleNode("@field") != null) return new PipelineFieldAction(pipeline, node);
         if (node.SelectSingleNode("@add") != null) return new PipelineAddAction(pipeline, node);
         throw new BMNodeException(node, "Don't know how to create an action for this node."); 
      }
   }

   public class PipelineFieldAction : PipelineDataAction
   {
      protected String toField;

      public PipelineFieldAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         toField = node.ReadStr("@field");
      }

      public override void HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = Convert(value);
         endPoint.SetField (toField, value);
      }
   }



   public class PipelineAddAction : PipelineDataAction
   {
      public PipelineAddAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      public override void HandleValue(PipelineContext ctx, String key, Object value)
      {
         endPoint.Add();
         endPoint.Clear();
      }
   }


}
