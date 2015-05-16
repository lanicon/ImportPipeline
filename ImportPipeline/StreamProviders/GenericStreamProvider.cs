using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Xml;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class GenericStreamProvider : IStreamProvidersRoot
   {
      List<IStreamProviderBase> roots;
      public readonly XmlNode contextNode;

      public GenericStreamProvider()
      { }
      public GenericStreamProvider(PipelineContext ctx, XmlNode node)
      {
         Init(ctx, node);
      }
      public void Init(PipelineContext ctx, XmlNode node)
      {
         roots = new List<IStreamProviderBase>();
         var nodeElt = (XmlElement)node;
         if (!tryFillFromUrlAttrib(ctx, nodeElt))
            if (!tryFillFromFileAttrib(ctx, nodeElt))
               if (!tryFromProviderNodes(ctx, nodeElt))
                  throw new BMNodeException(node, "Missing url/file attributes and provider-nodes.");
      }

      private bool tryFromProviderNodes(PipelineContext ctx, XmlElement nodeElt)
      {
         XmlNodeList nodes = nodeElt.SelectNodes("provider");
         if (nodes.Count == 0) return false;

         for (int i = 0; i < nodes.Count; i++)
         {
            roots.Add(createFromNode (ctx, (XmlElement)nodes[i], nodeElt));
         } 
         return true;
      }

      private IStreamProviderBase createFromNode(PipelineContext ctx, XmlElement providerNode, XmlElement parentNode)
      {
         if (providerNode.GetAttributeNode("url") != null)
            return new WebStreamProvider(ctx, providerNode, parentNode);

         if (providerNode.GetAttributeNode("file") != null || providerNode.GetAttributeNode("root") != null)
            return new FileCollectionStreamProvider (ctx, providerNode, parentNode);

         String type = providerNode.ReadStr ("@type");
         return ImportEngine.CreateObject<IStreamProviderBase> (type, ctx, providerNode, parentNode);
      }

      private bool tryFillFromFileAttrib(PipelineContext ctx, XmlElement nodeElt)
      {
         XmlNode attr = nodeElt.GetAttributeNode("file");
         if (attr == null) return false;
         roots.Add(new FileCollectionStreamProvider(ctx, nodeElt, nodeElt));
         return true;
      }


      private bool tryFillFromUrlAttrib(PipelineContext ctx, XmlElement nodeElt)
      {
         XmlNode attr = nodeElt.GetAttributeNode("url");
         if (attr == null) return false;
         roots.Add (new WebStreamProvider (ctx, nodeElt, nodeElt));
         return true;
      }

      public IEnumerable<IStreamProviderBase> GetRootElements(PipelineContext ctx)
      {
         if (roots != null)
            foreach (var root in roots) yield return root;
      }


      public IEnumerable<IStreamProvider> GetElements(PipelineContext ctx)
      {
         if (roots != null)
            foreach (var root in roots)
            {
               var p = root as IStreamProvider;
               if (p != null)
               {
                  yield return p;
                  continue;
               }
               var coll = (IStreamProviderCollection)root;
               foreach (var elt in coll.GetElements(ctx))
                  yield return (IStreamProvider)elt;
            }
      }
   }
}
