using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using System.Xml.XPath;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline.Datasources
{
   /// <summary>
   /// Selects an html/xml node and processes that node
   /// </summary>
   public class NodeSelector
   {
      public List<NodeSelector> Children;
      public List<NodeExporter> Outputs;
      public readonly XPathExpression Expr;

      public NodeSelector(String expr)
      {
         try
         {
            Expr = String.IsNullOrEmpty(expr) ? null : XPathExpression.Compile(expr);
         }
         catch (Exception e)
         {
            throw new BMException(e, "Compilation error in {0}: {1}", expr, e.Message);
         }
         Children = new List<NodeSelector>(0);
         Outputs = new List<NodeExporter>(0);
      }

      public void Process(PipelineContext ctx, INode node)
      {
         if (Expr == null)
         {
            for (int j = 0; j < Children.Count; j++) Children[j].Process(ctx, node);
            for (int j = 0; j < Outputs.Count; j++) Outputs[j].Export(ctx, node);
            return;
         }

         INode[] subs = node.SelectNodes(Expr);
         if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0)
         {
            var logger = ctx.DebugLog;
            logger.Log();
            logger.Log("Expr \"{0}\" returned {1} items", Expr.Expression, subs.Length);
            foreach (var elt in subs)
               logger.Log("-- {0}", elt);
         }
         for (int i = 0; i < subs.Length; i++)
         {
            for (int j = 0; j < Children.Count; j++) Children[j].Process(ctx, subs[i]);
            for (int j = 0; j < Outputs.Count; j++) Outputs[j].Export(ctx, subs[i]);
         }
      }

      public static NodeSelector Parse(XmlNode node)
      {
         NodeSelector ret = new NodeSelector(node.ReadStr("@node", null));
         foreach (XmlNode sub in node.SelectNodes("select"))
            ret.Children.Add(Parse(sub));

         ret.addOutputs(node, "@output", (k) => new NodeExporter(k));
         ret.addOutputs(node, "@output_text", (k) => new NodeTextExporter(k));
         ret.addOutputs(node, "@output_inner", (k) => new NodeInnerExporter(k));
         ret.addOutputs(node, "@output_outer", (k) => new NodeOuterExporter(k));

         ret.addOutputs(node, "output", (k) => new NodeExporter(k));
         ret.addOutputs(node, "output_text", (k) => new NodeTextExporter(k));
         ret.addOutputs(node, "output_inner", (k) => new NodeInnerExporter(k));
         ret.addOutputs(node, "output_outer", (k) => new NodeOuterExporter(k));
         return ret;
      }

      delegate NodeExporter CREATER(String key);
      private void addOutputs(XmlNode node, String expr, CREATER creater)
      {
         if (Outputs.Count > 0) return;

         if (expr.StartsWith("@"))
         {
            String key = node.ReadStr(expr, null);
            if (key != null) Outputs.Add(creater(key));
            return;
         }
         XmlNodeList nodes;
         nodes = node.SelectNodes(expr);
         foreach (XmlNode sub in nodes)
         {
            String key = sub.ReadStr("@key");
            Outputs.Add(creater(key));
         }
      }
   }
}
