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
      protected NamedItem(String name)
      {
         Name = name;
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

      public new T Add(T item)
      {
         namedItems.Add(item.Name, item);
         base.Add(item);
         return item;
      }

      /// <summary>
      /// Returns an element if it exists under that name, or raise an exception.
      /// </summary>
      public T GetByName(String name, bool mustExcept=true)
      {
         T item = namedItems.OptGetItem(name);
         if (item != null) return item;
         if (mustExcept)
            throw new BMException("Name '{0}' not found for type '{1}'.", name, typeof(T).FullName);
         return null;
      }

      /// <summary>
      /// <para>- If name!=null, the element associated with that name is returned or an exception is thrown</para>
      /// <para>- If altName != null, a check is done for altName. If found, it is returned.</para>
      /// <para>- Otherwise the 1st element is returned (if count==1), or an exception is thrown.</para>
      /// </summary>
      public T GetByNamesOrFirst(String name, String altName)
      {
         if (name != null) return GetByName(name);
         T item = null;
         if (altName != null)
         {
            if (namedItems.TryGetValue(altName, out item)) return item;
         }

         switch (namedItems.Count)
         {
            case 0: throw new BMException("'{0}' collection contains no elements. Cannot get the correct element.", typeof(T).FullName);
            case 1: return this[0];
            default: throw new BMException("'{0}' collection contains {1} elements. Cannot auto-configure the correct element, since it will be ambigious.", typeof(T).FullName, namedItems.Count);
         }
      }
   }

}
