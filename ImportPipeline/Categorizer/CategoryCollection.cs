using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;

namespace Bitmanager.ImportPipeline
{
   public class CategoryCollection : NamedItem
   {
      private enum CategoryMode { All, One };
      public readonly List<Category> Categories;
      private CategoryMode mode;

      public CategoryCollection(XmlNode node)
         : base(node)
      {
         XmlNodeList list = node.SelectNodes("category");
         Categories = new List<Category>(list.Count);
         foreach (XmlNode sub in list)
            Categories.Add(Category.Create (sub));
         mode = node.ReadEnum("@mode", CategoryMode.All);
      }

      public void HandleRecord(PipelineContext ctx)
      {
         IDataEndpoint ep = ctx.Action.Endpoint;
         JObject rec = (JObject)ep.GetField(null); 
         for (int i=0; i<Categories.Count; i++)
         {
            if (!Categories[i].HandleRecord(ctx, ep, rec)) continue;
            if (mode == CategoryMode.One) break;
         }
      }
   }
}
