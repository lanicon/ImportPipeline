using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Determines how a key should be compared
   /// </summary>
   public enum CompareType { String = 0x01, Int = 0x02, Long = 0x04, Double = 0x08, Date = 0x10, Descending = 0x10000, CaseInsensitive = 0x20000 };

   public class KeyAndType
   {
      public readonly JPath Key;
      public readonly CompareType Type;

      public KeyAndType(JPath k, CompareType t=0)
      {
         Key = k;
         Type = t;
      }
      public KeyAndType(String k, CompareType t = 0)
      {
         Key = new JPath(k);
         Type = t;
      }
      public KeyAndType(XmlNode node, bool mandatoryType)
      {
         Key = new JPath(node.ReadStr("@expr"));
         Type = mandatoryType ? node.ReadEnum<CompareType>("@type") : node.ReadEnum<CompareType>("@type", 0); 
      }

      public static List<KeyAndType> CreateKeyList(XmlNode root, String name, bool mandatory, bool mandatoryType)
      {
         XmlNodeList nodes = mandatory ? root.SelectMandatoryNodes(name) : root.SelectNodes(name);
         var ret = new List<KeyAndType>(nodes.Count);

         for (int i = 0; i < nodes.Count; i++)
            ret.Add(new KeyAndType(nodes[i], mandatoryType));
         return ret;
      }

   }
}
