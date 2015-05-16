using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Json;

namespace Bitmanager.ImportPipeline
{
   public abstract class Category
   {
      public Category[] SubCats;
      public readonly ICategorySelector Selector;
      public readonly String Field;
      public Category(XmlNode node)
      {
         Selector = CategorySelector.CreateSelectors(node);
         Field = node.ReadStr ("@dstfield");

         XmlNodeList subNodes = node.SelectNodes(node.Name);
         if (subNodes.Count > 0)
         {
            SubCats = new Category[subNodes.Count];
            for (int i = 0; i < subNodes.Count; i++) SubCats[i] = Create(subNodes[i]);
         }
     
      }

      public abstract bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec);

      protected void HandleSubcats (PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (SubCats == null) return;
         for (int i = 0; i < SubCats.Length; i++) SubCats[i].HandleRecord(ctx, ep, rec);
      }

      public static Category Create (XmlNode node)
      {
         var elt = (XmlElement)node;
         if (elt.HasAttribute("intcat")) return new IntCategory(node);
         if (elt.HasAttribute("dblcat")) return new DblCategory(node);
         return new StringCategory(node);
      }

   }

   public class StringCategory : Category
   {
      public readonly String Value;

      public StringCategory(XmlNode node)
         : base(node)
      {
         Value = node.ReadStr("@cat");
      }

      public override bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (!Selector.IsSelected(rec)) return false;

         if (SubCats != null) HandleSubcats(ctx, ep, rec);
         if (Value != null) ep.SetField(Field, Value, FieldFlags.Append, ";");
         return true;
      }
   }

   public class IntCategory : Category
   {
      public readonly long Value;

      public IntCategory(XmlNode node)
         : base(node)
      {
         Value = node.ReadInt64("@intcat");
      }

      public override bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (!Selector.IsSelected(rec)) return false;

         if (SubCats != null) HandleSubcats(ctx, ep, rec);
         if (Value != 0) ep.SetField(Field, Value, FieldFlags.ToArray);
         return true;
      }
   }

   public class DblCategory : Category
   {
      public readonly double Value;

      public DblCategory(XmlNode node)
         : base(node)
      {
         Value = node.ReadFloat("@dblcat");
      }

      public override bool HandleRecord(PipelineContext ctx, IDataEndpoint ep, JObject rec)
      {
         if (!Selector.IsSelected(rec)) return false;

         if (SubCats != null) HandleSubcats(ctx, ep, rec);
         if (Value != null) ep.SetField(Field, Value, FieldFlags.OverWrite);
         return true;
      }
   }
}
