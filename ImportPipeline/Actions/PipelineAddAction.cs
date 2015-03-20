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
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public enum KeyCheckMode
   {
      key = 1, date = 2
   }
   public class PipelineAddAction : PipelineAction
   {
      public CategoryCollection[] Categories;
      public PipelineAddAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
          Categories = loadCategories(pipeline, node);
      }

      internal PipelineAddAction(PipelineAddAction template, String name, Regex regex)
         : base(template, name, regex)
      {
         Categories = template.Categories;
      }

      static CategoryCollection[] loadCategories(Pipeline pipeline, XmlNode node)
      {
         String[] cats = node.ReadStr("@categories", null).SplitStandard();
         if (cats == null || cats.Length == 0) return null;

         CategoryCollection[] list = new CategoryCollection[cats.Length];
         var engine = pipeline.ImportEngine;
         for (int i = 0; i < cats.Length; i++)
         {
            list[i] = engine.Categories.GetByName(cats[i]);
         }
         return list;
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) { ctx.Skipped++; goto EXIT_RTN; }

         if (Categories != null)
            foreach (var cat in Categories) cat.HandleRecord(ctx);

         if (checkMode != 0)
         {
            Object res = base.handleCheck(ctx, value);
            if (res != null)
            {
               var existState = (ExistState)res;
               if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
               {
                  ctx.Skipped++;
                  goto EXIT_RTN;
               }
            }
         }
         ctx.IncrementAndLogAdd();
         endPoint.Add(ctx);
         endPoint.Clear();
         pipeline.ClearVariables();

      EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

   public class PipelineAddTemplate : PipelineTemplate
   {
      public PipelineAddTemplate(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
         template = new PipelineAddAction(pipeline, node);
      }

      public override PipelineAction OptCreateAction(PipelineContext ctx, String key)
      {
         if (!regex.IsMatch(key)) return null;
         return new PipelineAddAction((PipelineAddAction)template, key, regex);
      }
   }


}
