using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using Newtonsoft.Json.Linq;
using Bitmanager.ImportPipeline;
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline
{
   public class CatergorySelectorInt : CategorySelector
   {
      public readonly int Value;
      public CatergorySelectorInt(XmlNode node)
         : base(node)
      {
         Value = node.ReadInt("@intvalue");
      }



      public override bool IsSelectedToken(JToken val)
      {
         if (val == null) return false;
         switch (val.Type)
         {
            case JTokenType.Array:
               return IsSelectedArr((JArray)val);
            case JTokenType.Integer:
            case JTokenType.Float:
               return Value == (long)val;
            case JTokenType.String:
               long v;
               if (!Invariant.TryParse((String)val, out v)) return false;
               return v == Value;
            default:
               return false;
         }
      }
   }


   public class CatergorySelectorIntRange : CategorySelector
   {
      public readonly LongRange Range;
      public CatergorySelectorIntRange(XmlNode node)
         : base(node)
      {
         Range = new LongRange(node.ReadStr("@intrange"));
      }

      public override bool IsSelectedToken(JToken val)
      {
         if (val == null) return false;
         switch (val.Type)
         {
            case JTokenType.Array:
               return IsSelectedArr((JArray)val);
            case JTokenType.Integer:
            case JTokenType.Float:
               return Range.IsInRange((long)val);
            case JTokenType.String:
               return Range.IsInRange((String)val);
            default:
               return false;
         }
      }
   }

   public class CatergorySelectorDblRange : CategorySelector
   {
      public readonly DoubleRange Range;
      public CatergorySelectorDblRange(XmlNode node)
         : base(node)
      {
         Range = new DoubleRange(node.ReadStr("@dblrange"));
      }

      public override bool IsSelectedToken(JToken val)
      {
         if (val == null) return false;
         switch (val.Type)
         {
            case JTokenType.Array:
               return IsSelectedArr((JArray)val);
            case JTokenType.Integer:
            case JTokenType.Float:
               return Range.IsInRange((double)val);
            case JTokenType.String:
               return Range.IsInRange((String)val);
            default:
               return false;
         }
      }
   }

}
