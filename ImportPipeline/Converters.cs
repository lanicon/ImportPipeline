using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Globalization;
using System.Web;

namespace Bitmanager.ImportPipeline
{
   public class Converters : NamedAdminCollection<Converter>
   {
      public Converters(XmlNode collNode, String childrenNode, Func<XmlNode, Converter> factory, bool mandatory)
         : base(collNode, childrenNode, factory, mandatory)
      {
      }
      public Converter[] ToConverters (XmlNode node)
      {
         return ToConverters(readConverters(node));
      }
      public Converter[] ToConverters (String convertersStr)
      {
         if (String.IsNullOrEmpty(convertersStr)) return null;

         String[] arr = convertersStr.SplitStandard();

         var ret = new Converter[arr.Length];
         for (int i = 0; i < arr.Length; i++)
         {
            ret[i] = OptGetByName (arr[i]);
            if (ret[i] == null) throw new BMException("Cannot find converter '{0}'.", arr[i]);
         }
         return ret;
      }
      public static String readConverters(XmlNode node)
      {
         String s = node.OptReadStr("@converters", null);
         return s != null ? s : node.OptReadStr("@convert", null); 
      }
   }

   public abstract class Converter : NamedItem
   {
      public Converter(XmlNode node) : base(node) { }
 
      public abstract Object Convert (Object obj);
      public virtual void DumpMissed(PipelineContext ctx)
      {
      }


      public static Converter Create(XmlNode node)
      {
         String type = node.OptReadStr("@type", node.ReadStr("@name")).ToLowerInvariant();
         switch (type)
         {
            case "htmlencode": return new HtmlEncodeConverter(node);
            case "htmldecode": return new HtmlDecodeConverter(node);
            case "urlencode": return new UrlEncodeConverter(node);
            case "urldecode": return new UrlDecodeConverter(node);
            case "datetime": return new ToDateConverter(node, type);
            case "date": return new ToDateConverter(node, type);
            case "time": return new ToDateConverter(node, type);
            case "trim": return new TrimConverter(node);
            case "trimwhite": return new TrimWhiteConverter(node);
            case "lower": return new ToLowerConverter(node);
            case "upper": return new ToUpperConverter(node);
            case "double": return new ToDoubleConverter(node);
            case "int32": return new ToInt32Converter(node);
            case "int64": return new ToInt32Converter(node);
         }
         return Objects.CreateObject<Converter>(type, node);
      }
   }

   public class ToDateConverter : Converter
   {
      private string[] formats;
      private bool utc;
      public ToDateConverter(XmlNode node, String type)
         : base(node)
      {
         formats = node.OptReadStr("@formats", null).SplitStandard();
         utc = node.OptReadBool("@utc", false);
         if (type == "date") utc = false;
      }

      public override Object Convert(Object value)
      {
         if (value == null) return null;

         DateTime ret;
         String str = value as String;
         if (str != null)
         {
            if (formats != null)
            {
               if (DateTime.TryParseExact(str, formats, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AdjustToUniversal, out ret)) goto EXIT_RTN;
            }
            ret = Invariant.ToDateTime(str);
            goto EXIT_RTN;
         }

         ret = (DateTime)value;

      EXIT_RTN:
         return utc ? ret.ToUniversalTime() : ret;
      }
   }

   public abstract class ToNumConverter : Converter 
   {
      protected NumberFormatInfo numberFormat;
      public ToNumConverter(XmlNode node)
         : base(node)
      {
         numberFormat = CultureInfo.InvariantCulture.NumberFormat;
         String groupSep = node.OptReadStr("@groupsep", null);
         String decimalSep = node.OptReadStr("@decimalsep", null);
         if (groupSep != null || decimalSep != null)
         {
            numberFormat = (NumberFormatInfo)numberFormat.Clone();
            if (groupSep != null) { numberFormat.CurrencyGroupSeparator = groupSep; numberFormat.NumberDecimalSeparator = groupSep; }
            if (decimalSep != null) { numberFormat.CurrencyDecimalSeparator = decimalSep; numberFormat.NumberDecimalSeparator = decimalSep; }
         }
      }
   }
   public class ToDoubleConverter : ToNumConverter
   {
      public ToDoubleConverter(XmlNode node) : base(node)  {}

      public override Object Convert(Object value)
      {
         if (value == null) return null;

         double ret;
         String str = value as String;
         if (str != null)
         {
            if (double.TryParse(str, NumberStyles.Number, numberFormat, out ret))
               return ret;
         }
         Type t = value.GetType();
         if (t == typeof(double)) return (double)value;
         if (t == typeof(float)) return (double)(float)value;
         if (t == typeof(Int64)) return (double)(Int64)value;
         if (t == typeof(Int32)) return (double)(Int32)value;
         return (double)value;
      }
   }
   public class ToInt64Converter : ToNumConverter
   {
      public ToInt64Converter(XmlNode node) : base(node) {}

      public override Object Convert(Object value)
      {
         if (value == null) return null;

         Int64 ret;
         String str = value as String;
         if (str != null)
         {
            if (Int64.TryParse(str, NumberStyles.Number, numberFormat, out ret))
               return ret;
         }
         Type t = value.GetType();
         if (t == typeof(double)) return (Int64)(double)value;
         if (t == typeof(float)) return (Int64)(float)value;
         if (t == typeof(Int64)) return (Int64)(Int64)value;
         if (t == typeof(Int32)) return (Int64)(Int32)value;
         return (Int64)value;
      }
   }

   public class ToInt32Converter : ToNumConverter
   {
      public ToInt32Converter(XmlNode node) : base(node) {}

      public override Object Convert(Object value)
      {
         if (value == null) return null;

         Int32 ret;
         String str = value as String;
         if (str != null)
         {
            if (Int32.TryParse(str, NumberStyles.Number, numberFormat, out ret))
               return ret;
         }
         Type t = value.GetType();
         if (t == typeof(double)) return (Int32)(double)value;
         if (t == typeof(float)) return (Int32)(float)value;
         if (t == typeof(Int64)) return (Int32)(Int64)value;
         if (t == typeof(Int32)) return (Int32)(Int32)value;
         return (Int32)value;
      }
   }

   public class ToLowerConverter : Converter
   {
      public ToLowerConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return value.ToString().ToLowerInvariant();
      }
   }

   public class ToUpperConverter : Converter
   {
      public ToUpperConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return value.ToString().ToUpperInvariant();
      }
   }

   public class TrimConverter : Converter
   {
      public TrimConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return value.ToString().Trim();
      }
   }
   public class TrimWhiteConverter : Converter
   {
      public TrimWhiteConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return value.ToString().TrimWhiteSpace();
      }
   }

   public class HtmlEncodeConverter : Converter
   {
      public HtmlEncodeConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return HttpUtility.HtmlEncode(value.ToString());
      }
   }

   public class HtmlDecodeConverter : Converter
   {
      public HtmlDecodeConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return HttpUtility.HtmlDecode(value.ToString());
      }
   }

   public class UrlEncodeConverter : Converter
   {
      public UrlEncodeConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return HttpUtility.UrlEncode(value.ToString());
      }
   }

   public class UrlDecodeConverter : Converter
   {
      public UrlDecodeConverter(XmlNode node) : base(node) { }

      public override Object Convert(Object value)
      {
         if (value == null) return null;
         return HttpUtility.UrlDecode(value.ToString());
      }
   }

}
