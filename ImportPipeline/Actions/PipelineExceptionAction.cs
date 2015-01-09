using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;

namespace Bitmanager.ImportPipeline
{
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


   public class PipelineExceptionTemplate : PipelineTemplate
   {
      public PipelineExceptionTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineExceptionAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineExceptionAction((PipelineExceptionAction)template, key, regex);
      }
   }

}
