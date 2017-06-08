using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline.Datasources
{
   /// <summary>
   /// Exports the complete INode as a value
   /// </summary>
   public class NodeExporter
   {
      public readonly String Key;
      public NodeExporter(String key)
      {
         this.Key = key;
      }
      public virtual void Export(PipelineContext ctx, INode node)
      {
         ctx.Pipeline.HandleValue(ctx, Key, node);
      }
   }


   /// <summary>
   /// Exports the text for an INode
   /// </summary>
   public class NodeTextExporter : NodeExporter
   {
      public NodeTextExporter(String key) : base(key) { }
      public override void Export(PipelineContext ctx, INode node)
      {
         ctx.Pipeline.HandleValue(ctx, Key, node.InnerText());
      }
   }


   /// <summary>
   /// Exports the inner Xml/Html for an INode
   /// </summary>
   public class NodeInnerExporter : NodeExporter
   {
      public NodeInnerExporter(String key) : base(key) { }
      public override void Export(PipelineContext ctx, INode node)
      {
         ctx.Pipeline.HandleValue(ctx, Key, node.InnerXml());
      }
   }


   /// <summary>
   /// Exports the outer Xml/Html for an INode
   /// </summary>
   public class NodeOuterExporter : NodeExporter
   {
      public NodeOuterExporter(String key) : base(key) { }
      public override void Export(PipelineContext ctx, INode node)
      {
         ctx.Pipeline.HandleValue(ctx, Key, node.OuterXml());
      }
   }

}
