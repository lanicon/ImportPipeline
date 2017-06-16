using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.XPath;

namespace Bitmanager.ImportPipeline.Datasources
{
   /// <summary>
   /// Virtualizes an XmlNode or an HtmlNode
   /// </summary>
   public abstract class INode
   {
      protected static readonly INode[] EMPTY = new INode[0];
      public abstract INode[] SelectNodes(XPathExpression expr);
      public abstract String InnerText();
      public abstract String InnerXml();
      public abstract String OuterXml();
      public abstract Object WrappedNode();

      public String TrimToLength (String x, int len)
      {
         if (x == null || x.Length <= len) return x;
         return x.Substring(0, len)+ "...";
      }
   }

   /// <summary>
   /// Wraps an HtmlNode.
   /// Special care is taken to be able to select attributes (which are not HtmlNode's)
   /// </summary>
   public class HtmlNodeWrapper : INode
   {
      public readonly HtmlNode Wrapped;
      public readonly String Value;
      public readonly bool IsAttribute;
      public HtmlNodeWrapper(HtmlNode h)
      {
         Wrapped = h;
         IsAttribute = false;
         Value = h.InnerText;
      }
      public HtmlNodeWrapper(HtmlNodeNavigator h)
      {
         Wrapped = h.CurrentNode;
         IsAttribute = h.NodeType == System.Xml.XPath.XPathNodeType.Attribute;
         Value = IsAttribute ? h.Value : Wrapped.InnerText;
      }

      public override INode[] SelectNodes(XPathExpression expr)
      {
         var nav = Wrapped.CreateNavigator();
         var iter = nav.Select(expr);
         var ret = new INode[iter.Count];

         while (iter.MoveNext())
         {
            HtmlNodeNavigator n = (HtmlNodeNavigator)iter.Current;
            ret[iter.CurrentPosition - 1] = new HtmlNodeWrapper(n);
         }
         return ret;
      }

      public override string InnerText()
      {
         return Value;
      }

      public override string InnerXml()
      {
         return IsAttribute ? Value : Wrapped.InnerHtml;
      }

      public override string OuterXml()
      {
         return IsAttribute ? Value : Wrapped.OuterHtml;
      }

      public override object WrappedNode()
      {
         return Wrapped;
      }
      public override String ToString()
      {
         return String.Format("HtmlNode [name={0}, type={1}, value={2}]", Wrapped.Name, IsAttribute ? "attrib" : Wrapped.NodeType.ToString(), TrimToLength(Value, 50));
      }
   }

   /// <summary>
   /// Wraps an XmlNode.
   /// </summary>
   public class XmlNodeWrapper : INode
   {
      public readonly XmlNode Wrapped;
      public XmlNodeWrapper(XmlNode h)
      {
         Wrapped = h;
      }

      public override INode[] SelectNodes(XPathExpression expr)
      {
         var nav = Wrapped.CreateNavigator();
         var iter = nav.Select(expr);
         var ret = new INode[iter.Count];

         while (iter.MoveNext())
         {
            ret[iter.CurrentPosition - 1] = new XmlNodeWrapper(((IHasXmlNode)iter.Current).GetNode());
         }
         return ret;
      }

      public override string InnerText()
      {
         return Wrapped.InnerText;
      }

      public override string InnerXml()
      {
         return Wrapped.InnerXml;
      }

      public override string OuterXml()
      {
         return Wrapped.OuterXml;
      }

      public override object WrappedNode()
      {
         return Wrapped;
      }

      public override String ToString()
      {
         String v = Wrapped.Value;
         if (String.IsNullOrEmpty(v)) v = Wrapped.InnerText;
         if (String.IsNullOrEmpty(v)) v = Wrapped.OuterXml;
         return String.Format("XmlNode [name={0}, type={1}, value={2}]", Wrapped.Name, Wrapped.NodeType, TrimToLength(v, 50));
      }
   }
}
