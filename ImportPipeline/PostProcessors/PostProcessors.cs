using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

using Bitmanager.Elastic;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline
{
   public class PostProcessors
   {
      StringDict<IPostProcessor> postProcessors;
      public PostProcessors(ImportEngine engine, XmlNode collNode)
      {
         postProcessors = new StringDict<IPostProcessor>();
         if (collNode == null) return;

         var nodes = collNode.SelectNodes("postprocessor"); 
         for (int i=0; i<nodes.Count; i++)
         {
            XmlNode c = nodes[i];
            IPostProcessor p = ImportEngine.CreateObject<IPostProcessor> (c, engine, c);
            postProcessors.Add(p.Name, p);
         }
      }

      public IPostProcessor GetPostProcessor(string processor)
      {
         return postProcessors[processor];
      }
   }
}
