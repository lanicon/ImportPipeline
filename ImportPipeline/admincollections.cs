using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline
{
   public class NamedItem
   {
      public readonly String Name;
      protected NamedItem(XmlNode node)
      {
         Name = node.ReadStr("@name");
      }
      protected NamedItem(XmlNode node, String attr)
      {
         Name = node.ReadStr(attr);
      }
   }

   public class AdminCollection<T> : List<T>
   {
      public AdminCollection(XmlNode collNode, String childrenNode, Func<XmlNode, T> factory, bool mandatory)
      {
         if (collNode == null) return;
         XmlNodeList children = (mandatory) ? collNode.SelectMandatoryNodes(childrenNode) : collNode.SelectNodes(childrenNode);
         if (children.Count == 0) return;

         for (int i = 0; i < children.Count; i++)
         {
            Add(factory(children[i]));
         }
      }
   }

   public class NamedAdminCollection<T> : AdminCollection<T> where T : NamedItem
   {
      private StringDict<T> namedItems;
      public NamedAdminCollection(XmlNode collNode, String childrenNode, Func<XmlNode, T> factory, bool mandatory)
         : base(collNode, childrenNode, factory, mandatory)
      {
         namedItems = new StringDict<T>(Count);
         for (int i = 0; i < Count; i++)
         {
            T item = base[i];
            namedItems.Add(item.Name, item);
         }
      }

      public T OptGetByName(String name)
      {
         return namedItems.OptGetItem(name);
      }
      public T GetByName(String name)
      {
         T item = namedItems.OptGetItem(name);
         if (item != null) return item;
         throw new BMException("No element '{0}' found for type '{1}'.", name, typeof(T).FullName);
      }
   }

}
