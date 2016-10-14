/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
   public class RootStreamDirectory : StreamDirectory
   {
      protected readonly List<StreamDirectory> children;
      public readonly XmlNode contextNode;

      public RootStreamDirectory(PipelineContext ctx, XmlNode node)
         : base(ctx, node)
      {
         children = new List<StreamDirectory>();
         StreamDirectory x = createFromNode(ctx, ContextNode, ContextNode, false);
         if (x != null)
            children.Add(x);
         else
         {
            XmlNodeList nodes = ContextNode.SelectNodes("provider");
            for (int i = 0; i < nodes.Count; i++)
            {
               children.Add(createFromNode(ctx, (XmlElement)nodes[i], ContextNode, true));
            }
         }

         if (children.Count == 0)
            throw new BMNodeException(node, "No providers found. No <provider> child is found and no url/file/root attributes are found.");
      }

      public virtual void Dump(PipelineContext ctx)
      {
         ctx.ImportLog.Log("Dumping {0} stream providers.", children.Count);
         foreach (var d in children)
         {
            ctx.ImportLog.Log("-- {0}", d.ToString());
         }
      }

      private StreamDirectory createFromNode(PipelineContext ctx, XmlElement providerNode, XmlElement parentNode, bool mustExcept)
      {
         String type = providerNode.ReadStr("@type", null);
         if (type != null && providerNode.LocalName == "provider")  //Node could be a datasource, where type describes the type of the datasource
            return ImportEngine.CreateObject<StreamDirectory>(type, ctx, providerNode, parentNode);

         if (providerNode.GetAttributeNode("file") != null || providerNode.GetAttributeNode("root") != null)
            return new FileStreamDirectory(ctx, providerNode, parentNode);

         if (providerNode.GetAttributeNode("url") != null)
            return new WebStreamDirectory(ctx, providerNode, parentNode);

         if (mustExcept) XmlUtils.ThrowMissingException(providerNode, "@type, @file, @root or @url");
         return null;
      }

      public override IEnumerator<object> GetChildren(PipelineContext ctx)
      {
         return this.children.GetEnumerator();
      }
   }

   public class StreamDirectoryWrapper : StreamDirectory
   {
      protected readonly IStreamProvider provider;
      public StreamDirectoryWrapper (IStreamProvider Element): base(null, Element.ContextNode)
      {
         provider = Element;
      }
      public override IEnumerator<Object> GetChildren(PipelineContext ctx)
      {
         yield return provider; 
      }
   }

   public abstract class StreamDirectory
   {
      public readonly XmlElement ContextNode;

      public StreamDirectory(PipelineContext ctx, XmlNode node)
      {
         ContextNode = (XmlElement)node;
      }

      public abstract IEnumerator<Object> GetChildren(PipelineContext ctx);
      public virtual IEnumerable<IStreamProvider> GetProviders (PipelineContext ctx)
      {
         return CreateRecursiveEnumerator(ctx, GetChildren(ctx));
      }

      public IEnumerable<IStreamProvider> CreateRecursiveEnumerator(PipelineContext ctx, IEnumerable<Object> e)
      {
         return new EnumerableWrapper<IStreamProvider>(new StreamDirectoryEnumerator(ctx, this, e.GetEnumerator()));
      }
      public IEnumerable<IStreamProvider> CreateRecursiveEnumerator(PipelineContext ctx, IEnumerator<Object> e)
      {
         return new EnumerableWrapper<IStreamProvider>(new StreamDirectoryEnumerator(ctx, this, e));
      }

      internal Queue<Object> forcedNext;
      public virtual void SetNextProvider(StreamDirectory d)
      {
         if (d == null) return;
         if (forcedNext == null) forcedNext = new Queue<Object>();
         forcedNext.Enqueue(d);
      }
      public virtual void SetNextProvider(IStreamProvider p)
      {
         if (p == null) return;
         if (forcedNext == null) forcedNext = new Queue<Object>();
         forcedNext.Enqueue(p);
      }
   }

}
