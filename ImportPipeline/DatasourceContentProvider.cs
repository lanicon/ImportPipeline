using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public interface IDatasourceFeederElement
   {
      XmlNode Context {get;}
      Object  Element {get;}
   }
   public interface IDatasourceFeeder : IEnumerable<IDatasourceFeederElement>
   {
      void Init(PipelineContext ctx, XmlNode node);
   }


   public class FeederElementBase: IDatasourceFeederElement
   {
      public XmlNode Context { get; private set; }
      public Object Element { get; private set; }
      public FeederElementBase(XmlNode ctx, Object element)
      {
         Context = ctx;
         Element = element;
      }
      public override string ToString()
      {
         return Element.ToString();
      }
   }
}
