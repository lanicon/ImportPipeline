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
   public interface IDatasourceFeeder
   {
      void Init(PipelineContext ctx, XmlNode node);
      IEnumerable<IDatasourceFeederElement> GetElements (PipelineContext ctx);
   }


   public class FeederElementBase: IDatasourceFeederElement
   {
      public XmlNode Context { get; protected set; }
      public Object Element { get; protected set; }
      public FeederElementBase(XmlNode ctx, Object element)
      {
         Context = ctx;
         Element = element;
      }
      protected FeederElementBase(XmlNode ctx)
      {
         Context = ctx;
      }
      protected FeederElementBase()
      {
      }
      public override string ToString()
      {
         return Element.ToString();
      }
   }
}
