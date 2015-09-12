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
         var act = PipelineAction.GetActionType(node); 
         switch (act)
         {
            case _ActionType.Add: return new PipelineAddTemplate(pipeline, node);
            case _ActionType.Clear: return new PipelineClearTemplate(pipeline, node);
            case _ActionType.Nop: return new PipelineNopTemplate(pipeline, node);
            case _ActionType.Field: return new PipelineFieldTemplate(pipeline, node);
            case _ActionType.Emit: return new PipelineEmitTemplate(pipeline, node);
            case _ActionType.Except: return new PipelineExceptionTemplate(pipeline, node);
            case _ActionType.Del: return new PipelineDeleteTemplate(pipeline, node);
            case _ActionType.Cat: return new PipelineCategorieTemplate(pipeline, node);
            case _ActionType.Cond: return new PipelineConditionTemplate(pipeline, node);
            case _ActionType.CheckExist: return new PipelineCheckExistTemplate(pipeline, node);
         }
         act.ThrowUnexpected();
         return null;
      }

      public override string ToString()
      {
         return String.Format("{0}: {1}", this.GetType().Name, template);
      }
   }

}
