using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;


namespace Bitmanager.ImportPipeline
{
   public class ComplexConverter : Converter
   {
      public enum Mode { Flatten, First, Last };
      public readonly Mode ConvertMode;
      public readonly String Sep;
      public ComplexConverter(XmlNode node)
         : base(node)
      {
         ConvertMode = node.ReadEnum<Mode>("@mode");
         Sep = node.OptReadStr("@sep", "; ");
      }


      public override Object Convert(PipelineContext ctx, Object value)
      {
         if (value == null)
         {
            throw new BMException ("Cannot convert a null array.");
         }

         Array arr = value as Array;
         if (arr != null)
         {
            if (arr.Length==0) return null;

            switch (ConvertMode)
            {
               case Mode.Flatten:
                  StringBuilder sb = new StringBuilder();
                  foreach (var obj in arr)
                  {
                     if (sb.Length > 0) sb.Append(Sep);
                     sb.Append(obj == null ? "null" : obj.ToString());
                  }
                  return sb.ToString();
               case Mode.First: return arr.GetValue(0);
               case Mode.Last: return arr.GetValue(arr.Length-1);
               default: throw new BMException("Unexpected enum {0}: {1}", typeof(Mode).Name, ConvertMode);
            }
         }
         JArray jarr = value as JArray;
         if (jarr == null) return value;

         if (jarr.Count == 0) return null;

         switch (ConvertMode)
         {
            case Mode.Flatten:
               StringBuilder sb = new StringBuilder();
               foreach (var obj in jarr)
               {
                  if (sb.Length > 0) sb.Append(Sep);
                  sb.Append(obj == null ? "null" : obj.ToString());
               }
               return sb.ToString();
            case Mode.First: return jarr[0];
            case Mode.Last: return jarr[jarr.Count - 1];
            default: throw new BMException("Unexpected enum {0}: {1}", typeof(Mode).Name, ConvertMode);
         }
      }

      public override Object ConvertScalar(PipelineContext ctx, Object obj)
      {
         JValue jv = obj as JValue;
         return jv == null ? obj : jv.ToNative();
      }


   }
}
