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
      //public Converter[] ToConverters (XmlNode node)
      //{
      //   return ToConverters(readConverters(node));
      //}
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
         return s != null ? s : node.OptReadStr("@converter", null); 
      }
   }

   public abstract class Converter : NamedItem
   {
      protected bool needValue;
      public Converter(XmlNode node) : base(node) { needValue = true; }

      protected bool TryConvertArray(PipelineContext ctx, Object value, out Object convertedArray)
      {
         convertedArray = value;
         if (value == null) return true;
         Array arr = value as Array;
         if (arr != null) 
         {
            convertedArray = convertArray (ctx, arr);
            return true;
         }
         JArray jarr = value as JArray;
         if (jarr != null) 
         {
            convertedArray = convertArray (ctx, jarr);
            return true;
         }
         return false;
      }
      protected Object[] convertArray(PipelineContext ctx, JArray src)
      {
         Object[] ret = new Object[src.Count];
         for (int i = 0; i < src.Count; i++)
         {
            ret[i] = ConvertScalar(ctx, src[i].ToNative());
         }
         return ret;
      }
      protected Object[] convertArray(PipelineContext ctx, Array src)
      {
         Object[] ret = new Object[src.Length];
         for (int i = 0; i < src.Length; i++)
         {
            ret[i] = ConvertScalar(ctx, src.GetValue(i));
         }
         return ret;
      }

      public virtual Object Convert(PipelineContext ctx, Object value)
      {
         if (value == null)
         {
            return needValue ? null : ConvertScalar(ctx, value); 
         }
         Array arr = value as Array;
         if (arr != null)  return convertArray(ctx, arr);
         JArray jarr = value as JArray;
         if (jarr != null) return convertArray(ctx, jarr);
         return ConvertScalar(ctx, value);
      }
      public abstract Object ConvertScalar(PipelineContext ctx, Object obj);
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
            case "dateonly":
            case "datetime":
            case "date": 
            case "time": return new ToDateConverter(node, type);
            case "datepart": return new ToDatePartConverter(node, type);
            case "trim": return new TrimConverter(node);
            case "trimwhite": return new TrimWhiteConverter(node);
            case "lower": return new ToLowerConverter(node);
            case "upper": return new ToUpperConverter(node);
            case "string": return new ToStringConverter(node);
            case "double": return new ToDoubleConverter(node);
            case "int32": return new ToInt32Converter(node);
            case "int64": return new ToInt32Converter(node);
            case "split": return new SplitConverter(node);
            case "format": return new FormatConverter(node);
         }
         return Objects.CreateObject<Converter>(type, node);
      }
   }

   public enum DateMode {none, ToUtc, ToLocal};
   public class ToDateConverter : Converter
   {
      protected class _TZ
      {
         public readonly String Name;
         public readonly String Offset;

         public _TZ(XmlNode x)
         {
            Name = x.ReadStr("@name");
            Offset = x.ReadStr("@offset");
         }
      }
      static String[] stdFormats = new String[] {
        "yyyy-MM-ddTHH:mm:ss.FFFFFFFzzzzzz",
        "yyyy-MM-ddTHH:mm:ss.FFFFFFF",
        "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ",
        "ddd, d MMM yyyy HH:mm:ss zzz",
        "ddd d MMM yyyy HH:mm:ss zzz",
        "d MMM yyyy HH:mm:ss zzz",
        "ddd, MMM d HH:mm:ss zzz yyyy",
        "ddd MMM d HH:mm:ss zzz yyyy",
        "HH:mm:ss.FFFFFFF",
        "HH:mm:ss.FFFFFFFZ",
        "HH:mm:ss.FFFFFFFzzzzzz",
        "yyyy-MM-dd",
        "yyyy-MM-ddZ",
        "yyyy-MM-ddzzzzzz",
        "yyyy-MM",
        "yyyy-MMZ",
        "yyyy-MMzzzzzz",
        "yyyy",
        "yyyyZ",
        "yyyyzzzzzz",
        "--MM-dd",
        "--MM-ddZ",
        "--MM-ddzzzzzz",
        "---dd",
        "---ddZ",
        "---ddzzzzzz",
        "--MM--",
        "--MM--Z",
        "--MM--zzzzzz"
      };
      protected string[] formats;
      protected _TZ[] timezones;
      protected DateMode mode;
      public ToDateConverter(XmlNode node, String type)
         : base(node)
      {
         XmlNodeList nodes;
         this.formats = stdFormats;
         bool includeStd = node.OptReadBool("includestandard", true);
         String[] formats = node.OptReadStr("@formats", null).SplitStandard();
         if (formats != null)
         {
            this.formats = includeStd ? formats.ToList().Concat(stdFormats).ToArray() : formats;
         }
         else
         {
            nodes = node.SelectNodes("format");
            if (nodes.Count > 0)
            {
               var tmp = new List<String>();
               foreach (XmlNode fn in nodes) tmp.Add(fn.Value);
               if (includeStd) tmp.Concat(stdFormats);
               this.formats = tmp.ToArray();
            }
         }

         nodes = node.SelectNodes("timezone");
         if (nodes.Count > 0)
         {
            var tmp = new List<_TZ>();
            foreach (XmlNode x in nodes) tmp.Add (new _TZ(x));
            timezones = tmp.ToArray();
         }


         if (type == "dateonly") mode = DateMode.none;
         else
         {
            if (node.SelectSingleNode("@mode") == null)
               mode = node.OptReadBool("@utc", false) ? DateMode.ToUtc : DateMode.none;
            else
               mode = node.OptReadEnum("@mode", DateMode.ToUtc); 
         }

         //var logger = Logs.DebugLog;
         //logger.Log ("{0}: Dumping formats...", this.GetType().Name);
         //foreach (var s in this.formats) logger.Log("-- " + s);
      }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         DateTime ret;
         String str = value as String;
         if (str != null)
         {
            str = replaceTimeZone(str);
            if (formats != null)
            {
               if (DateTime.TryParseExact(str, formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out ret)) goto EXIT_RTN;
            }
            ret = Invariant.ToDateTime(str);
            goto EXIT_RTN;
         }

         ret = (DateTime)value;

      EXIT_RTN:
         switch (mode)
         {
            case DateMode.ToLocal: return ret.ToLocalTime();
            case DateMode.ToUtc: return ret.ToUniversalTime();
         }
         return ret;
      }

      protected String replaceTimeZone(String str)
      {
         if (timezones == null) return str;
         foreach (var tz in timezones)
         {
            int ix = str.IndexOf(tz.Name);
            if (ix < 0) continue;
            return str.Substring(0, ix) + tz.Offset + str.Substring(ix + tz.Name.Length);
         }
         return str;
      }
   }


   public class ToDatePartConverter : ToDateConverter
   {
      private enum SelectMode {y, m, d, format};
      private string select;
      private SelectMode selectMode;
      public ToDatePartConverter(XmlNode node, String type)
         : base(node, type)
      {
         select = node.ReadStr ("@select");
         switch (select.ToLowerInvariant())
         {
            case "y": selectMode = SelectMode.y; break;
            case "m": selectMode = SelectMode.m; break;
            case "d": selectMode = SelectMode.d; break;
            default: selectMode = SelectMode.format; break;
         }
      }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         DateTime ret = (DateTime)base.ConvertScalar(ctx, value);
         switch (selectMode)
         {
            case SelectMode.y: return ret.Year;
            case SelectMode.m: return ret.Month;
            case SelectMode.d: return ret.Day;
            default: return ret.ToString(select);
         }
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

      public override Object ConvertScalar(PipelineContext ctx, Object value)
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

      public override Object ConvertScalar(PipelineContext ctx, Object value)
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
      public ToInt32Converter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
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

   public class ToStringConverter : Converter
   {
      public ToStringConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         return Invariant.ToString(value);
      }
   }

   public class ToLowerConverter : Converter
   {
      public ToLowerConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : v.ToLowerInvariant();
      }
   }

   public class ToUpperConverter : Converter
   {
      public ToUpperConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : v.ToUpperInvariant();
      }
   }

   public class TrimConverter : Converter
   {
      public TrimConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : v.Trim();
      }
   }
   public class TrimWhiteConverter : Converter
   {
      public TrimWhiteConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : v.TrimWhiteSpace();
      }
   }

   public class HtmlEncodeConverter : Converter
   {
      public HtmlEncodeConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : HttpUtility.HtmlEncode(v); 
      }
   }

   public class HtmlDecodeConverter : Converter
   {
      public HtmlDecodeConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : HttpUtility.HtmlDecode(v);
      }
   }

   public class UrlEncodeConverter : Converter
   {
      public UrlEncodeConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : HttpUtility.UrlEncode(v);
      }
   }

   public class UrlDecodeConverter : Converter
   {
      public UrlDecodeConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         return v == null ? value : HttpUtility.UrlDecode(v);
      }
   }

   public class SplitConverter : Converter
   {
      bool trim = true;
      char sep = ';';
      public SplitConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         String v = value as String;
         if (v == null) return value;

         String[] arr = v.Split(sep);
         for (int i = 0; i < arr.Length; i++) arr[i] = arr[i].Trim();
         return arr;
      }
   }

   public class FormatConverter : Converter
   {
      enum FormatFlags { None = 0, NeedArguments = 1, NeedValue = 2, NeedAll = 3 };
      private FormatArgument[] arguments;
      private String format;
      private FormatFlags flags;
      public FormatConverter(XmlNode node) : base(node) {
         format = node.ReadStr("@format");
         flags = node.OptReadEnum("@flags", FormatFlags.NeedArguments);
         needValue = (flags & FormatFlags.NeedValue) != 0;

         String[] args = node.ReadStr("@arguments").SplitStandard();
         if (args != null && args.Length > 0)
         {
            arguments = new FormatArgument[args.Length];
            for (int i = 0; i < args.Length; i++)
            {
               arguments[i] = createArgument(args[i]);
            }
         }
      }

      private FormatArgument createArgument(string arg)
      {
         if (String.IsNullOrEmpty(arg)) throw new Exception ("FormatArgument cannot be empty.");
         String lcArg = arg.ToLowerInvariant();
         if (lcArg[lcArg.Length-1] == ')')
         {
            if (lcArg.StartsWith("field("))
               return new FormatArgument_Field(lcArg.Substring (6, lcArg.Length-7).Trim());
            if (lcArg.StartsWith("key("))
               return new FormatArgument_Key(lcArg.Substring (4, lcArg.Length-5).Trim());
            if (lcArg.StartsWith("f("))
               return new FormatArgument_Field(lcArg.Substring (2, lcArg.Length-3).Trim());
            if (lcArg.StartsWith("k("))
               return new FormatArgument_Key(lcArg.Substring (2, lcArg.Length-3).Trim());
         }
         throw new BMException ("Invalid FormatArgument specifier: '{0}'.", arg);
      }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         if ((flags & FormatFlags.NeedValue) != 0 && value == null) return null;
         if (arguments == null)
            return Invariant.Format(format, value);

         Object[] argArr = new Object[arguments.Length + 1];
         argArr[0] = value;
         for (int i = 0; i < arguments.Length; i++)
         {
            argArr[i+1] = arguments[i].GetArgument(ctx);
            if ((flags & FormatFlags.NeedArguments) == 0) continue;
            if (argArr[i + 1] == null) return null;
            String tmp = argArr[i + 1] as String;
            if (tmp != null && tmp.Length == 0) return null;
         }
         return Invariant.Format(format, argArr);
      }
   }


   class FormatArgument
   {
      public virtual Object GetArgument(PipelineContext ctx)
      {
         return null;
      }
   }
   class FormatArgument_Key : FormatArgument
   {
      protected readonly String key;
      public FormatArgument_Key(String key)
      {
         this.key = key;
      }

      public override Object GetArgument(PipelineContext ctx)
      {
         return ctx.Pipeline.GetVariable(key);
      }
   }
   class FormatArgument_Field : FormatArgument
   {
      protected readonly String field;
      public FormatArgument_Field(String field)
      {
         this.field = field;
      }

      public override Object GetArgument(PipelineContext ctx)
      {
         return ctx.Action.Endpoint.GetField (field);
      }
   }
}
