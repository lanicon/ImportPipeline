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
using System.Xml;
using System.IO;
using HtmlAgilityPack;
using System.Web;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline
{
   public class HtmlProcessor
   {
      public enum _TagType { Unknown = 1, Unwanted = 2, Block = 4, Inline = 8 };

      static readonly StringDict<_TagType> tagDict;
      public static _TagType GetTagType(String tag)
      {
         return GetTagType(tagDict, tag);
      }
      public static _TagType GetTagType(StringDict<_TagType> tagDict, String tag)
      {
         if (String.IsNullOrEmpty(tag)) return _TagType.Unknown;
         _TagType ret;
         if (tagDict.TryGetValue(tag.ToLowerInvariant(), out ret))
            return ret;
         return _TagType.Unknown;
      }

      public int numParts;
      public int numAttachments;
      public bool IsTextMail;
      private readonly bool removeTitleNodes = false;
      private readonly bool removeMetaNodes = false;
      private static Logger logger = Logs.CreateLogger("tika", "htmlprocessor");
      private String _newHtml;
      public readonly List<KeyValuePair<String, String>> Properties;
      public readonly HtmlDocument Document;
      public HtmlNode HtmlNode { get; private set; }
      public HtmlNode BodyNode { get; private set; }
      public HtmlNode HeadNode { get; private set; }

      public HtmlProcessor(String html)
         : this(fromString(html))
      {
      }
      static HtmlDocument fromString(String html)
      {
         var doc = new HtmlDocument();
         doc.LoadHtml(html);
         return doc;
      }
      public HtmlProcessor(HtmlDocument doc)
      {
         Properties = new List<KeyValuePair<string, string>>();
         var docNode = doc.DocumentNode;
         BodyNode = docNode.SelectSingleNode("//body");
         if (BodyNode != null)
            HtmlNode = findParentHtmlNode(BodyNode, docNode);
         else
            HtmlNode = BodyNode = docNode;

         if (HtmlNode == null) goto EXIT_RTN;
         HeadNode = HtmlNode.SelectSingleNode("head");
         if (HeadNode == null) goto EXIT_RTN; ;

         processMetaNodes(HeadNode.SelectNodes("meta"));
         processTitleNodes(HeadNode.SelectNodes("title"));
         removeEmptyTextNodes(HeadNode.ChildNodes);
         undupMailNodes();
         removeEmptyTextNodes(BodyNode.SelectNodes("//text()"));
         PatchTargets("_blank");
         computeNumAttachments();
      EXIT_RTN:
         Document = doc;
      }

      private bool isTextNode(HtmlNode mailNode)
      {
         HtmlNode child = mailNode.SelectSingleNode("*");
         if (child == null) return false;
         if (String.Equals("pre", child.Name, StringComparison.OrdinalIgnoreCase)) return true;
         if (!String.Equals("p", child.Name, StringComparison.OrdinalIgnoreCase)) return false;
         child = child.SelectSingleNode("*");
         if (child == null) return false;
         return String.Equals("pre", child.Name, StringComparison.OrdinalIgnoreCase);
      }

      public void PatchTargets(String target)
      {
         if (BodyNode == null) return;
         HtmlNodeCollection anchors = BodyNode.SelectNodes("//a");
         if (anchors == null) return;
         foreach (HtmlNode anchor in anchors)
         {
            anchor.SetAttributeValue("target", target);
         }
      }

      public void undupMailNodes()
      {
         if (BodyNode == null) return;
         HtmlNodeCollection nodes = BodyNode.SelectNodes("div[@class='email-entry']");
         int N = nodes == null ? 0 : nodes.Count;
         numParts = N;
         switch (N)
         {
            case 0:
               IsTextMail = isTextNode(BodyNode);
               return;
            case 1:
               IsTextMail = isTextNode(nodes[0]);
               return;
         }

         int maxIdx = -1;
         int maxCnt = 0;
         for (int i = 0; i < nodes.Count; i++)
         {
            int cnt = nodes[i].Descendants().Count();
            if (cnt <= maxCnt) continue;
            maxCnt = cnt;
            maxIdx = i;
         }

         IsTextMail = isTextNode(nodes[maxIdx]);
         for (int i = 0; i < nodes.Count; i++)
         {
            if (maxIdx == i) continue;
            BodyNode.RemoveChild(nodes[i], false);
         }
      }

      public static bool QuessIsHtml(String html)
      {
         if (String.IsNullOrEmpty(html)) return false;
         int tags = 0;
         for (int i = 0; i < html.Length; i++)
         {
            switch (html[i])
            {
               default:
                  if (i > 1000) return false;
                  continue;
               case '<':
                  ++tags;
                  break;
               case '>':
                  ++tags;
                  if (i > 2 && html[i - 1] == '/') tags += 2;
                  break;
            }
            if (tags >= 8) return true;
         }
         return false;
      }


      private void computeNumAttachments()
      {
         if (BodyNode == null) return;
         HtmlNodeCollection nodes = BodyNode.SelectNodes("//p[@class='email-attachment-name']");
         numAttachments = nodes == null ? 0 : nodes.Count;
      }

      public String GetInnerBody()
      {
         return (BodyNode == null) ? String.Empty : BodyNode.InnerHtml;
      }
      public String GetInnerHead()
      {
         return (HeadNode == null) ? String.Empty : HeadNode.InnerHtml;
      }
      public String GetNewHtml()
      {
         if (_newHtml != null) return _newHtml;

         return _newHtml = Document.DocumentNode.OuterHtml;
      }

      private static HtmlNode findParentHtmlNode(HtmlNode bodyNode, HtmlNode doc)
      {
         for (HtmlNode p = bodyNode.ParentNode; p != null; p = p.ParentNode)
         {
            if (p.Name.Equals("html", StringComparison.InvariantCultureIgnoreCase)) return p;
         }
         return doc;
      }

      public static String GetAbstractFromText(String text, int maxLength, int delta)
      {
         //StringBuilder sb = new StringBuilder();
         if (text == null || text.Length <= maxLength + delta) return text;

         int from = maxLength - delta;
         int to = maxLength + delta;

         //String inspect = txt.Substring(from, 2 * delta);

         int bestSentencePos = -1;
         int bestWordPos = -1;
         for (int i = from; i < to; i++)
         {
            switch (text[i])
            {
               default: continue;
               case '.':
               case '?':
               case '!': setBestPos(ref bestSentencePos, i, maxLength); continue;

               case ' ':
               case ';':
               case ',':
               case ':':
               case '\r':
               case '\n':
               case '\t': setBestPos(ref bestWordPos, i, maxLength); continue;
            }
         }
         if (bestSentencePos < 0)
         {
            bestSentencePos = (bestWordPos < 0) ? maxLength - 1 : bestWordPos;
         }
         //inspect = txt.Substring(0, bestSentencePos) + " \u2026";
         //return inspect;
         return text.Substring(0, bestSentencePos) + " \u2026";     //Add ellipsis
      }
      private static void setBestPos(ref int pos, int i, int maxLength)
      {
         int d1 = Math.Abs(maxLength - pos);
         int d2 = Math.Abs(maxLength - i);
         if (d2 < d1) pos = i;
      }

      public String GetAbstract(int maxLength, int delta)
      {
         return GetAbstractFromText(GetText(maxLength + delta), maxLength, delta);
      }


      private static readonly char[] TRIMCHARS = { ' ', '\t', '\r', '\n' };
      private static bool appendInnerText(StringBuilder bld, HtmlNode node, int maxLength = -1)
      {
         switch (node.NodeType)
         {
            case HtmlNodeType.Text:
               String s = HttpUtility.HtmlDecode(node.InnerText);
               if (bld.Length == 0)
               {
                  if (String.IsNullOrEmpty(s)) return true;
                  s = s.TrimStart(TRIMCHARS);
               }
               if (String.IsNullOrEmpty(s)) return true;
               bld.Append(s);
               if (maxLength > 0 && bld.Length >= maxLength) return false;
               return true;
            case HtmlNodeType.Comment: return true;
         }

         var type = GetTagType(node.Name);
         if ((type & _TagType.Unwanted) != 0) return true;
         if ((type & _TagType.Inline) == 0) optAppendBlank(bld);

         if (!node.HasChildNodes) return true;

         foreach (HtmlNode c in node.ChildNodes)
         {
            if (!appendInnerText(bld, c, maxLength)) return false;
         }
         if ((type & _TagType.Inline) == 0) optAppendBlank(bld);
         return true;
      }
      private static void optAppendBlank(StringBuilder sb)
      {
         if (sb.Length > 0 && sb[sb.Length - 1] != ' ')
            sb.Append(' ');
      }

      public String GetText(HtmlNode n, int maxLength)
      {
         if (n == null) return null;
         StringBuilder bld = new StringBuilder();
         appendInnerText(bld, n, maxLength);

         int i = bld.Length - 1;
         for (; i >= 0; i--)
         {
            switch (bld[i])
            {
               case ' ':
               case '\r':
               case '\n':
               case '\t': continue;
            }
            break;
         }

         return i < 0 ? null : bld.ToString(0, i + 1);
      }
      public String GetText(int maxLength)
      {
         return GetText(BodyNode, maxLength);
      }
      public String GetText()
      {
         return GetText(BodyNode, int.MaxValue);
      }



      private void processMetaNodes(HtmlNodeCollection metaNodes)
      {
         if (metaNodes == null) return;
         List<HtmlNode> list = toList(metaNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlNode node = list[i];
            addMetaData(node.GetAttributeValue("name", null), node.GetAttributeValue("content", null));
            if (removeMetaNodes) node.Remove();
         }
      }
      //String s = HttpUtility.HtmlDecode(node.InnerText);
      private void removeEmptyTextNodes(HtmlNodeCollection textNodes)
      {
         if (textNodes == null) return;
         List<HtmlNode> list = toList(textNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlTextNode txtNode = list[i] as HtmlTextNode;

            if (txtNode == null) continue;
            //            logger.Log("textNode[{0}]={1} [{2}]", i, onlyWhiteSpace(txtNode.Text), txtNode.Text);
            if (onlyWhiteSpace(txtNode.Text))
               txtNode.Remove();
         }
      }

      private bool onlyWhiteSpace(String txt, bool decoded = false)
      {
         if (String.IsNullOrEmpty(txt)) return true;

         for (int i = 0; i < txt.Length; i++)
         {
            switch (txt[i])
            {
               //case '&':
               //   if (decoded)
               //   {
               //      logger.Log("Non empty char={0:X}", (int)txt[i]);
               //      return false;
               //   }
               //   return onlyWhiteSpace(HttpUtility.HtmlDecode(txt), true);

               case (char)0xA0:
               case ' ':
               case '\r':
               case '\n':
               case '\t': continue;
            }
            //            logger.Log("Non empty char={0:X}", (int)txt[i]);
            return false;
         }
         return true;
      }
      private void processTitleNodes(HtmlNodeCollection titleNodes)
      {
         if (titleNodes == null) return;
         List<HtmlNode> list = toList(titleNodes);
         for (int i = 0; i < list.Count; i++)
         {
            HtmlNode node = list[i];
            addMetaData("title", node.InnerText);
            if (removeTitleNodes) node.Remove();
         }
      }

      private void addMetaData(String key, String value)
      {
         if (String.IsNullOrEmpty(key)) return;
         //if (Properties.ContainsKey(key)) return;
         if (String.IsNullOrEmpty(value)) return;
         //Properties.Add(key, value);
         Properties.Add(new KeyValuePair<string, string>(key, value));
      }


      private static List<HtmlNode> toList(HtmlNodeCollection coll)
      {
         if (coll == null) return new List<HtmlNode>();
         return coll.ToList();
      }
      static void splitAndAdd(StringDict<_TagType> dict, String tags, _TagType type)
      {
         int prev = -1;
         for (int i = 0; i < tags.Length; i++)
         {
            switch (tags[i])
            {
               case ' ':
               case ',':
               case ';':
                  if (prev < 0) continue;
                  dict[tags.Substring(prev, i - prev)] = type;
                  prev = -1;
                  continue;
               default:
                  if (prev < 0) prev = i;
                  continue;
            }
         }
         if (prev >= 0)
            dict[tags.Substring(prev)] = type;
      }
      static HtmlProcessor()
      {
         var dict = new StringDict<_TagType>();
         splitAndAdd(dict, "b big i small tt", _TagType.Inline);
         splitAndAdd(dict, "abbr acronym cite code dfn em kbd strong samp var", _TagType.Inline);
         splitAndAdd(dict, "a, bdo, br, img, map, q, script, span, sub, sup", _TagType.Inline);
         splitAndAdd(dict, "button, input, label, select, textarea", _TagType.Inline);

         splitAndAdd(dict, "address article aside blockquote canvas dd div dl fieldset figcaption", _TagType.Block);
         splitAndAdd(dict, "figure footer form h1 h2 h3 h4 h5 h6 header hgroup hr main nav", _TagType.Block);
         splitAndAdd(dict, "ol output p pre section table tfoot ul video", _TagType.Block);

         splitAndAdd(dict, "style script noscript object", _TagType.Inline | _TagType.Unwanted);
         tagDict = dict;
      }
   }

}
