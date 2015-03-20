using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class CatergoryAndSelector : ICategorySelector
   {
      private List<ICategorySelector> items;

      public CatergoryAndSelector(System.Xml.XmlNode node)
      {
         items = new List<ICategorySelector>();
         CategorySelector.CreateChildSelectors(items, node);
      }

      public bool IsSelected(JObject obj)
      {
         for (int i = 0; i < items.Count; i++)
         {
            if (!items[0].IsSelected(obj)) return false;
         }
         return items.Count > 0;
      }

   }

   public class CatergoryOrSelector : ICategorySelector
   {
      private List<ICategorySelector> items;

      public CatergoryOrSelector(XmlNode node)
      {
         items = new List<ICategorySelector>();
         CategorySelector.CreateChildSelectors(items, node);
      }

      public CatergoryOrSelector(List<ICategorySelector> list)
      {
         items = list;
      }

      public bool IsSelected(JObject obj)
      {
         for (int i = 0; i < items.Count; i++)
         {
            if (items[0].IsSelected(obj)) return true;
         }
         return false;
      }

   }
}
