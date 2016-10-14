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

using Bitmanager.ImportPipeline;
using Bitmanager.Core;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using System.Threading.Tasks;
using System.Xml;
using System.Net;
using System.IO;
using HtmlAgilityPack;
using System.Text.RegularExpressions;
using System.Web;
using System.Threading;
using Bitmanager.Elastic;
using Bitmanager.ImportPipeline.StreamProviders;

namespace Bitmanager.ImportPipeline
{
   public class XmlDatasource : StreamDatasourceBase
   {
      private bool dumpReader;

      public XmlDatasource(): base(false, false)
      { }

      public override void Init(PipelineContext ctx, XmlNode node)
      {
         base.Init(ctx, node);
         dumpReader = node.ReadBool("@debug", false);
      }


      private StringDict getAttributes(XmlNode node)
      {
         StringDict ret = new StringDict();
         if (node == null) return ret;
         var coll = node.Attributes;
         for (int i = 0; i < coll.Count; i++)
         {
            var att = coll[i];
            if (att.LocalName.Equals("url", StringComparison.InvariantCultureIgnoreCase)) continue;
            if (att.LocalName.Equals("baseurl", StringComparison.InvariantCultureIgnoreCase)) continue;
            ret[att.LocalName] = att.Value;
         }
         return ret;
      }

      protected override void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm)
      {
         List<String> keys = new List<string>();
         List<String> values = new List<String>();
         int lvl = -1;

         XmlReader rdr = XmlReader.Create(strm);

         Logger l = ctx.DebugLog;
         while (rdr.Read())
         {
            if (dumpReader) l.Log("{0}: {1}, {2} [{3}]", rdr.Name, rdr.NodeType, rdr.IsEmptyElement, rdr.Value);
            switch (rdr.NodeType)
            {
               case XmlNodeType.CDATA:
               case XmlNodeType.Text:
               case XmlNodeType.Whitespace:
               case XmlNodeType.SignificantWhitespace:
                  if (lvl <= 0) continue;
                  values[lvl] = values[lvl] + rdr.Value;
                  continue;
               case XmlNodeType.Element:
                  lvl++;
                  if (lvl >= keys.Count) {keys.Add(null); values.Add(null);}
                  if (lvl == 0)
                     keys[0] = rdr.Name;
                  else
                  {
                     keys[lvl] = keys[lvl - 1] + "/" + rdr.Name;
                     if (lvl == 1) ctx.IncrementEmitted();
                  }

                  //l.Log("{0}: [{1}, {2}]", lvl, keys[lvl], rdr.NodeType);
                  bool isEmpty = rdr.IsEmptyElement;  //cache this value: after reading the attribs its value is lost 
                  if (rdr.AttributeCount > 0)
                  {
                     String pfx = keys[lvl] + "/@";
                     for (int j=0; j<rdr.AttributeCount; j++)
                     {
                        rdr.MoveToNextAttribute();
                        sink.HandleValue(ctx, pfx + rdr.Name,  rdr.Value);
                     }
                  }
                  if (!isEmpty) continue;

                  //l.Log("{0}: [{1}]", keys[lvl], rdr.NodeType);
                  sink.HandleValue(ctx, keys[lvl], null);
                  lvl--;

                  continue;
               case XmlNodeType.EndElement:
                  //l.Log("{0}: [{1}]", keys[lvl], rdr.NodeType);
                  sink.HandleValue(ctx, keys[lvl], values[lvl]);
                  values[lvl] = null;
                  lvl--;
                  continue;
            }
         }
         rdr.Close();
      }

   }
}
