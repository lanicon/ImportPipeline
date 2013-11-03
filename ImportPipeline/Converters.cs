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
         return ToConverters (node.OptReadStr("@converters", null));
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
   }

   public abstract class Converter : NamedItem
   {
      public Converter(XmlNode node) : base(node) { }
 
      public abstract Object Convert (Object obj);

      public static Converter Create(XmlNode node)
      {
         String type = node.OptReadStr("@type", node.ReadStr("@name")).ToLowerInvariant();
         switch (type)
         {
            case "datetime": return new ToDateConverter(node, type);
            case "date": return new ToDateConverter(node, type);
            case "time": return new ToDateConverter(node, type);
            case "trim": return new TrimConverter(node);
            case "lower": return new ToLowerConverter(node);
            case "upper": return new ToUpperConverter(node);
            case "double": return new ToDoubleConverter(node);
         }
         throw new BMNodeException(node, "Don't know how to create a Converter with type={0}.", type);
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

   public class ToDoubleConverter : Converter
   {
      private NumberFormatInfo numberFormat;
      public ToDoubleConverter(XmlNode node)
         : base(node)
      {
         numberFormat = CultureInfo.InvariantCulture.NumberFormat;
         String groupSep   = node.OptReadStr("@groupsep", null);
         String decimalSep = node.OptReadStr("@decimalsep", null);
         if (groupSep != null || decimalSep != null)
         {
            numberFormat = (NumberFormatInfo)numberFormat.Clone();
            if (groupSep != null)   { numberFormat.CurrencyGroupSeparator = groupSep; numberFormat.NumberDecimalSeparator = groupSep; }
            if (decimalSep != null) { numberFormat.CurrencyDecimalSeparator = decimalSep; numberFormat.NumberDecimalSeparator = decimalSep; }
         }
      }

      public override Object Convert(Object value)
      {
         if (value == null) return null;

         double ret;
         String str = value as String;
         if (str != null)
         {
            if (double.TryParse (str, NumberStyles.Number, numberFormat, out ret))
               return ret;
         }

         return (double)value;
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

}
