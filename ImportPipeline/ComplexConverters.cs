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
      private delegate Object dlg_worker(PipelineContext ctx, Object value);

      public enum Mode { Flatten, First, Last, Count };
      public readonly Mode ConvertMode;
      public readonly String Sep;
      private readonly dlg_worker fnWorker;

      public ComplexConverter(XmlNode node)
         : base(node)
      {
         ConvertMode = node.ReadEnum<Mode>("@mode");
         Sep = node.OptReadStr("@sep", "; ");
         switch (ConvertMode)
         {
            default: throw ConvertMode.UnexpectedException();
            case Mode.Count: fnWorker = doCount; break;
            case Mode.Flatten: fnWorker = doFlatten; break;
            case Mode.First: fnWorker = doFirst; break;
            case Mode.Last: fnWorker = doLast; break;
         }
      }


      public override Object Convert(PipelineContext ctx, Object value)
      {
         return fnWorker(ctx, value);
      }


      private Object doFlatten(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         Array arr = value as Array;
         if (arr != null)
         {
            if (arr.Length == 0) return null;

            StringBuilder sb = new StringBuilder();
            foreach (var obj in arr)
            {
               if (sb.Length > 0) sb.Append(Sep);
               sb.Append(obj == null ? "null" : obj.ToString());
            }
            return sb.ToString();
         }

         JArray jarr = value as JArray;
         if (jarr != null)
         {
            if (jarr.Count == 0) return null;
            StringBuilder sb = new StringBuilder();
            foreach (var obj in jarr)
            {
               if (sb.Length > 0) sb.Append(Sep);
               sb.Append(obj == null ? "null" : obj.ToString());
            }
            return sb.ToString();
         }

         JObject jobj = value as JObject;
         if (jobj != null)
         {
            if (jobj.Count == 0) return null;
            StringBuilder sb = new StringBuilder();
            foreach (var v in jobj)
            {
               if (sb.Length > 0) sb.Append(Sep);
               sb.Append(v.Value == null ? "null" : v.Value.ToString());
            }
            return sb.ToString();
         }

         return ConvertScalar(ctx, value);
      }

      private Object doFirst(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         Array arr = value as Array;
         if (arr != null)
         {
            return (arr.Length == 0) ? null : arr.GetValue(0);
         }

         JArray jarr = value as JArray;
         if (jarr != null)
         {
            return (jarr.Count == 0) ? null : jarr[0];
         }

         JObject jobj = value as JObject;
         if (jobj != null)
         {
            return (jobj.Count == 0) ? null : jobj.Values().First();
         }

         return ConvertScalar(ctx, value);
      }

      private Object doLast(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         Array arr = value as Array;
         if (arr != null)
         {
            return (arr.Length == 0) ? null : arr.GetValue(arr.Length - 1);
         }

         JArray jarr = value as JArray;
         if (jarr != null)
         {
            return (jarr.Count == 0) ? null : jarr[jarr.Count - 1];
         }

         JObject jobj = value as JObject;
         if (jobj != null)
         {
            return (jobj.Count == 0) ? null : jobj.Values().Last();
         }

         return ConvertScalar(ctx, value);
      }

      private Object doCount(PipelineContext ctx, Object value)
      {
         if (value == null) return 0;
         Array arr = value as Array;
         if (arr != null)
         {
            return arr.Length;
         }

         JArray jarr = value as JArray;
         if (jarr != null)
         {
            return jarr.Count;
         }

         JObject jobj = value as JObject;
         if (jobj != null)
         {
            return jobj.Count;
         }

         return 1;
      }
      public override Object ConvertScalar(PipelineContext ctx, Object obj)
      {
         if (ConvertMode == Mode.Count)
            return obj == null ? 0 : 1;
         JValue jv = obj as JValue;
         return jv == null ? obj : jv.ToNative();
      }


   }
}
