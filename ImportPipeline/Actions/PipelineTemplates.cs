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
      protected PipelineAction template;
      public PipelineTemplate(Pipeline pipeline, XmlNode node)
      {
         XmlElement e = (XmlElement)node;
         Expr = node.ReadStr("@expr");
         e.SetAttribute("key", Expr);
         regex = new Regex(Expr, RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
      }

      public abstract PipelineAction OptCreateAction(PipelineContext ctx, String key);

      public static PipelineTemplate Create(Pipeline pipeline, XmlNode node)
      {
         _InternalActionType act = PipelineAction.GetActionType(node); 
         switch (act)
         {
            case _InternalActionType.Add: return new PipelineAddTemplate(pipeline, node);
            case _InternalActionType.Nop: return new PipelineNopTemplate(pipeline, node);
            case _InternalActionType.Field: return new PipelineFieldTemplate(pipeline, node);
            case _InternalActionType.Emit: return new PipelineEmitTemplate(pipeline, node);
            case _InternalActionType.Except: return new PipelineExceptionTemplate(pipeline, node);
         }
         throw new Exception ("Unexpected _InternalActionType: " + act);
      }

      public override string ToString()
      {
         return String.Format("{0}: {1}", this.GetType().Name, template);
      }
   }

}
