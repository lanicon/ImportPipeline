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
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.Web;
using System.Reflection;

namespace Bitmanager.ImportPipeline
{
   public class Converters : NamedAdminCollection<Converter>
   {
      public Converters(XmlNode collNode, String childrenNode, Func<XmlNode, Converter> factory, bool mandatory)
         : base(collNode, childrenNode, factory, mandatory)
      {
         Converter.AddDefaultConverters(this);
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
            ret[i] = GetByName (arr[i], false);
            if (ret[i] != null) continue;
            dumpConverters();
            throw new BMException("Cannot find converter '{0}'.", arr[i]);
         }
         return ret;
      }
      public static String readConverters(XmlNode node)
      {
         String s = node.ReadStr("@converters", null);
         return s != null ? s : node.ReadStr("@converter", null); 
      }

      private void dumpConverters ()
      {
         Logger logger = Logs.ErrorLog;
         logger.Log("Dumping {0} converters", this.Count);
         foreach (var item in this)
         {
            logger.Log("-- Converter[{0}] = {1}", item.Name, item.GetType().Name);
         }
      }
   }

   public abstract class Converter : NamedItem
   {
      protected bool needValue;
      public Converter(XmlNode node) : base(node) { needValue = true; }
      public Converter(String name) : base(name) { needValue = true; }

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


      static ConverterFactory[] arr = {
                new ConverterFactory ("normalize", typeof(NormalizeConverter)),
                new ConverterFactory ("htmlencode", typeof(HtmlEncodeConverter)),
                new ConverterFactory ("htmldecode", typeof(HtmlDecodeConverter)),
                new ConverterFactory ("urlencode", typeof(UrlEncodeConverter)),
                new ConverterFactory ("urldecode", typeof(UrlDecodeConverter)),
                new ConverterFactory ("dateonly", typeof(ToDateConverter)),
                new ConverterFactory ("datetime", typeof(ToDateConverter)),
                new ConverterFactory ("date", typeof(ToDateConverter)),
                new ConverterFactory ("time", typeof(ToDateConverter)),
                new ConverterFactory ("datepart", typeof(ToDatePartConverter), false),
                new ConverterFactory ("trim", typeof(TrimConverter)),
                new ConverterFactory ("trimwhite", typeof(TrimWhiteConverter)),
                new ConverterFactory ("lower", typeof(ToLowerConverter)),
                new ConverterFactory ("upper", typeof(ToUpperConverter)),
                new ConverterFactory ("string", typeof(ToStringConverter)),
                new ConverterFactory ("nullifempty", typeof(ToNullIfEmptyConverter)),
                new ConverterFactory ("double", typeof(ToDoubleConverter)),
                new ConverterFactory ("int32", typeof(ToInt32Converter)),
                new ConverterFactory ("int64", typeof(ToInt64Converter)),
                new ConverterFactory ("split", typeof(SplitConverter)),
                new ConverterFactory ("htmltotext", typeof(HtmlToTextConverter)),
                new ConverterFactory ("format", typeof(FormatConverter), false),
                new ConverterFactory ("clone", typeof(CloneConverter)),
                new ConverterFactory ("canonicalize", typeof(CanonicalizeConverter))
      };

      public static void AddDefaultConverters (Converters coll)
      {
         for (int i = 0; i < arr.Length; i++)
         {
            if (!arr[i].Auto) continue;
            String type = arr[i].Name;
            if (coll.Contains(type)) continue;
            var converter = arr[i].Create();
            if (converter == null) continue;
            coll.Add(converter);
         }
      }
      public static Converter Create(XmlNode node)
      {
         String type = node.ReadStr("@type", node.ReadStr("@name")).ToLowerInvariant();
         for (int i=0; i<arr.Length; i++)
         {
            if (arr[i].Name!=type) continue;
            return arr[i].Create(node);
         }
         return Objects.CreateObject<Converter>(type, node);
      }
   }
   [Flags]
   public enum DateMode {none=0, ToUtc=1, ToLocal=2, ToUnspec=0x100};
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
      public ToDateConverter(String type) : base(type)
      {
         this.formats = stdFormats;
         mode = (type == "dateonly") ? DateMode.none : DateMode.ToUtc;
      }

      public ToDateConverter(XmlNode node, String type)
         : base(node)
      {
         XmlNodeList nodes;
         this.formats = stdFormats;
         bool includeStd = node.ReadBool("includestandard", true);
         String[] formats = node.ReadStr("@formats", null).SplitStandard();
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
               mode = node.ReadBool("@utc", false) ? DateMode.ToUtc : DateMode.none;
            else
               mode = node.ReadEnum("@mode", DateMode.ToUtc); 
         }

         //var logger = Logs.DebugLog;
         //logger.Log ("{0}: Dumping formats...", this.GetType().Name);
         //foreach (var s in this.formats) logger.Log("-- " + s);
      }

      protected void throwConvert(String toWhat, Object value)
      {
         String t;
         if (value==null)
         {
            t = "null";
         }
         else
         {
            JValue jv = value as JValue;
            t = jv==null ? value.GetType().Name : jv.Type.ToString();
         }
         throw new BMException ("Cannot convert a value with type [{0}] into a [{1}].", t, toWhat);
      }

      static System.DateTime dtEpoch = new DateTime(1970,1,1,0,0,0,0,System.DateTimeKind.Utc);
      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         DateTime ret;
         String sValue = null;
         double dblDate = 0.0;
         if (value == null) return null;

         JValue jv = value as JValue;
         if (jv != null)
         {
            switch (jv.Type)
            {
               default:
                  goto CANNOT_CONVERT;
               case JTokenType.Date:
                  ret = (DateTime)jv;
                  goto EXIT_RTN;
               case JTokenType.Integer:
                  dblDate = (long)jv;
                  goto FROM_DBL;
               case JTokenType.Float:
                  dblDate = (double)jv;
                  goto FROM_DBL;
               case JTokenType.String:
                  sValue = (String)jv;
                  goto FROM_STR;
            }
         }

         switch (Type.GetTypeCode(value.GetType()))
         {
            default:
               goto CANNOT_CONVERT;
            case TypeCode.DateTime:
               ret = (DateTime)value;
               goto EXIT_RTN;
            case TypeCode.String:
               sValue = (String)value;
               goto FROM_STR;
            case TypeCode.Double:
               dblDate = (double)value;
               goto FROM_DBL;
            case TypeCode.Int32:
               dblDate = (int)value;
               goto FROM_DBL;
            case TypeCode.Int64:
               dblDate = (long)value;
               goto FROM_DBL;
         }

      CANNOT_CONVERT:
         throwConvert("DateTime", value);

      FROM_STR:
         if (String.IsNullOrEmpty(sValue)) return null;
         sValue = replaceTimeZone(sValue);
         if (formats != null)
         {
            if (DateTime.TryParseExact(sValue, formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out ret)) goto EXIT_RTN;
         }
         ret = Invariant.ToDateTime(sValue);
         goto EXIT_RTN;

      FROM_DBL:
         //1000000 days is beyond 2100 when it is a COM date, but its in the 2nd day since epoch for linux. So lets decide on that limit.
         ret = (dblDate < 1000000.0) ? DateTime.FromOADate(dblDate) : dtEpoch.AddSeconds(dblDate);
         goto EXIT_RTN;


      EXIT_RTN:
         switch (mode & (DateMode.ToLocal | DateMode.ToUtc))
         {
            case DateMode.ToLocal: ret = ret.ToLocalTime(); break;
            case DateMode.ToUtc: ret = ret.ToUniversalTime(); break;
         }
         if ((mode & DateMode.ToUnspec) != 0)
         {
            ret = new DateTime (ret.Ticks, DateTimeKind.Unspecified);
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
      public ToNumConverter(String name)
         : base(name)
      {
         numberFormat = CultureInfo.InvariantCulture.NumberFormat;
      }
      public ToNumConverter(XmlNode node)
         : base(node)
      {
         numberFormat = CultureInfo.InvariantCulture.NumberFormat;
         String groupSep = node.ReadStr("@groupsep", null);
         String decimalSep = node.ReadStr("@decimalsep", null);
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
      public ToInt64Converter(XmlNode node) : base(node) { }
      public ToInt64Converter(String name) : base(name) { }

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
      public ToInt32Converter(string name) : base (name) {}

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

   public class ToNullIfEmptyConverter : Converter
   {
      public ToNullIfEmptyConverter(XmlNode node) : base(node) { }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         if (value == null) return null;
         return Invariant.ToString(value).ToNullIfEmpty();
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

   public class CanonicalizeConverter : Converter
   {
      char sep = ';';
      public CanonicalizeConverter(XmlNode node) : base(node) { }

      public override object Convert(PipelineContext ctx, object value)
      {
         JToken tk = value as JToken;
         if (tk == null) return value;
         return tk.Canonicalize();
      }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         //Not used since we override Convert...
         return null;
      }
   }

   public class CloneConverter : Converter
   {
      char sep = ';';
      public CloneConverter(XmlNode node) : base(node) { }

      public override object Convert(PipelineContext ctx, object value)
      {
         JToken tk = value as JToken;
         if (tk != null) return tk.DeepClone();
         var c = value as ICloneable;
         if (c != null) return c.Clone();

         return value;
      }

      public override Object ConvertScalar(PipelineContext ctx, Object value)
      {
         //Not used since we override Convert...
         return null;
      }
   }

   public class FormatConverter : Converter
   {
      enum FormatFlags { None = 0, NeedArguments = 1, NeedValue = 2, NeedAll = 3 };
      private FormatArgument[] arguments;
      private String format;
      private FormatFlags flags;
      public FormatConverter(XmlNode node)
         : base(node)
      {
         format = node.ReadStr("@format");
         flags = node.ReadEnum("@flags", FormatFlags.NeedArguments);
         needValue = (flags & FormatFlags.NeedValue) != 0;

         String[] args = node.ReadStr("@arguments", null).SplitStandard();
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


   class ConverterFactory
   {
      public readonly String Name;
      Type type;
      ConstructorInfo typeConstructor;
      ConstructorInfo nodeConstructor1, nodeConstructor2;

      public readonly bool Auto;

      static Type[] types_node = new Type[1] { typeof(XmlNode) };
      static Type[] types_nodeWithType = new Type[2] { typeof(XmlNode), typeof(String) };
      static Type[] types_type = new Type[1] { typeof(String) };


      public ConverterFactory (String name, Type type, bool auto = true)
      {
         this.Auto = auto;
         this.Name = name;
         this.type = type;
         nodeConstructor1 = type.GetConstructor(types_node);
         nodeConstructor2 = type.GetConstructor(types_nodeWithType);
         typeConstructor = type.GetConstructor(types_type);
         if (nodeConstructor1 == null && nodeConstructor2 == null) throw new BMException("Type {0} has no constructor (XmlNode, String) or (XmlNode).", type);
      }

      public Converter Create()
      {
         if (typeConstructor == null)
         {
            XmlDocument doc = new XmlDocument();
            XmlElement elt = doc.CreateElement ("dummy");
            elt.SetAttribute("name", Name);
            return Create(elt);
         }
         return (Converter)typeConstructor.Invoke(new Object[1] { Name });
      }
      public Converter Create(XmlNode node)
      {
         if (nodeConstructor1 != null) return (Converter) nodeConstructor1.Invoke(new Object[1] { node });
         return (Converter) nodeConstructor2.Invoke(new Object[2] { node, Name });
      }
   }
}
