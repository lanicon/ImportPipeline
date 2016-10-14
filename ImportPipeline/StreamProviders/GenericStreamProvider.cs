///*
// * Licensed to De Bitmanager under one or more contributor
// * license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright
// * ownership. De Bitmanager licenses this file to you under
// * the Apache License, Version 2.0 (the "License"); you may
// * not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */

//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Xml;
//using Bitmanager.Core;
//using Bitmanager.IO;
//using Bitmanager.Xml;

//namespace Bitmanager.ImportPipeline.StreamProviders
//{
//   public class GenericStreamProvider : IStreamProvidersRoot
//   {
//      List<IStreamProviderBase> roots;
//      public readonly XmlNode contextNode;

//      public GenericStreamProvider()
//      { }
//      public GenericStreamProvider(PipelineContext ctx, XmlNode node)
//      {
//         Init(ctx, node);
//      }
//      public void Init(PipelineContext ctx, XmlNode node)
//      {
//         roots = new List<IStreamProviderBase>();
//         var nodeElt = (XmlElement)node;
//         if (!tryFillFromUrlAttrib(ctx, nodeElt))
//            if (!tryFillFromFileOrRootAttrib(ctx, nodeElt))
//               if (!tryFromProviderNodes(ctx, nodeElt))
//                  throw new BMNodeException(node, "Missing url/file attributes and provider-nodes.");
//      }

//      public static void DumpRoots (PipelineContext ctx, IStreamProvidersRoot roots)
//      {
//         Logger lg = ctx.ImportLog;
//         var list = roots.GetRootElements(ctx).ToList();
//         lg.Log("Loaded {0} provider roots:", list.Count);
//         foreach (var x in list)
//            lg.Log("-- {0}", x);
//      }

//      private bool tryFromProviderNodes(PipelineContext ctx, XmlElement nodeElt)
//      {
//         XmlNodeList nodes = nodeElt.SelectNodes("provider");
//         if (nodes.Count == 0) return false;

//         for (int i = 0; i < nodes.Count; i++)
//         {
//            roots.Add(createFromNode (ctx, (XmlElement)nodes[i], nodeElt));
//         } 
//         return true;
//      }

//      private IStreamProviderBase createFromNode(PipelineContext ctx, XmlElement providerNode, XmlElement parentNode)
//      {
//         if (providerNode.GetAttributeNode("url") != null)
//            return new WebStreamProvider(ctx, providerNode, parentNode);

//         if (providerNode.GetAttributeNode("file") != null || providerNode.GetAttributeNode("root") != null)
//            return new FileCollectionStreamProvider (ctx, providerNode, parentNode);

//         String type = providerNode.ReadStr ("@type");
//         return ImportEngine.CreateObject<IStreamProviderBase> (type, ctx, providerNode, parentNode);
//      }

//      private bool tryFillFromFileOrRootAttrib(PipelineContext ctx, XmlElement nodeElt)
//      {
//         if (null != nodeElt.GetAttributeNode("file")) goto ACCEPT;
//         if (null != nodeElt.GetAttributeNode("root")) goto ACCEPT;
//         return false;

//         ACCEPT:
//         roots.Add(new FileCollectionStreamProvider(ctx, nodeElt, nodeElt));
//         return true;
//      }


//      private bool tryFillFromUrlAttrib(PipelineContext ctx, XmlElement nodeElt)
//      {
//         XmlNode attr = nodeElt.GetAttributeNode("url");
//         if (attr == null) return false;
//         roots.Add (new WebStreamProvider (ctx, nodeElt, nodeElt));
//         return true;
//      }

//      public IEnumerable<IStreamProviderBase> GetRootElements(PipelineContext ctx)
//      {
//         if (roots != null)
//            foreach (var root in roots) yield return root;
//      }


//      public IEnumerable<IStreamProvider> GetElements(PipelineContext ctx)
//      {
//         if (roots != null)
//            foreach (var root in roots)
//            {
//               var p = root as IStreamProvider;
//               if (p != null)
//               {
//                  yield return p;
//                  continue;
//               }
//               var coll = (IStreamProviderCollection)root;
//               foreach (var elt in coll.GetElements(ctx))
//                  yield return (IStreamProvider)elt;
//            }
//      }
//   }
//}
