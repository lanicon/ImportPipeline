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

namespace Bitmanager.ImportPipeline
{
   public abstract class PipelineTemplate
   {
      public readonly String Expr;
      protected Regex regex;
      protected String convertersName, endpointName;
      protected Pipeline pipeline;

      public PipelineTemplate(Pipeline pipeline, XmlNode node)
      {
         this.pipeline = pipeline;
         Expr = node.ReadStr("@expr");
         regex = new Regex(Expr, RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
         endpointName = PipelineAction.ReadParameters(pipeline, node, out convertersName);
      }

      public abstract PipelineAction OptCreateAction(PipelineContext ctx, String key);

      public static PipelineTemplate Create(Pipeline pipeline, XmlNode node)
      {
         if (node.SelectSingleNode("@field") != null) return new PipelineFieldTemplate(pipeline, node);
         if (node.SelectSingleNode("@add") != null) return new PipelineAddTemplate(pipeline, node);
         throw new BMNodeException(node, "Don't know how to create a template for this node.");
      }

      public override string ToString()
      {
         return String.Format("{0}: (expr={1}, endpoint={2}, conv={3}", this.GetType().Name, Expr, endpointName, convertersName);
      }

   }

   public class PipelineFieldTemplate : PipelineTemplate
   {
      protected String field;

      public PipelineFieldTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         field = node.ReadStr("@field");
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;

         String enpointName = regex.Replace(key, this.endpointName);
         String convertersName = this.convertersName == null ? null : regex.Replace(key, this.convertersName);
         String fieldName = regex.Replace(key, this.field);

         return new PipelineFieldAction(pipeline, key, enpointName, convertersName, fieldName);
      }
      public override string ToString()
      {
         return base.ToString() + ", field=" + field + ")";
      }
   }

   public class PipelineAddTemplate : PipelineTemplate
   {
      public PipelineAddTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;

         String enpointName = regex.Replace(key, this.endpointName);
         String convertersName = this.convertersName == null ? null : regex.Replace(key, this.convertersName);
         return new PipelineAddAction(pipeline, key, enpointName, convertersName);
      }

      public override string ToString()
      {
         return base.ToString() + ")";
      }
   }

   public class PipelineNopTemplate : PipelineTemplate
   {
      public PipelineNopTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineNopAction(key);
      }

      public override string ToString()
      {
         return base.ToString() + ")";
      }
   }
}
