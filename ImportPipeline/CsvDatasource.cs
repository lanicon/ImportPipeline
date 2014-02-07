using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using LumenWorks.Framework.IO.Csv;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.IO;
using System.Globalization;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public class CsvDatasource: Datasource
   {
      String file;
      char delimChar, quoteChar, commentChar;
      bool hasHeaders, trim;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         file = ctx.ImportEngine.Xml.CombinePath (node.ReadStr("@file"));
         hasHeaders = node.OptReadBool("@headers", false);
         trim = node.OptReadBool("@trim", true);
         delimChar = readChar(node, "@dlm", ',');
         quoteChar = readChar(node, "@quote", '"');
         commentChar = readChar(node, "@comment", '#');
      }

      internal static char readChar(XmlNode node, String attr, char def)
      {
         String v = node.OptReadStr (attr, null);
         if (v==null) return def;

         int x;
         switch (v.Length)
         {
            case 1: return v[0];
            case 4:
               if (v.StartsWith(@"0x", StringComparison.InvariantCultureIgnoreCase)) goto TRY_CONVERT;
               goto ERROR;
            case 6:
               if (v.StartsWith(@"0x", StringComparison.InvariantCultureIgnoreCase)) goto TRY_CONVERT;
               if (v.StartsWith(@"\u", StringComparison.InvariantCultureIgnoreCase)) goto TRY_CONVERT;
               goto ERROR;
         }
         goto ERROR;

         TRY_CONVERT:
         if (int.TryParse(v.Substring(2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out x))
            return (char)x;

      ERROR:
         throw new BMNodeException (node, "Invalid character({0}) at expression {1}. Must be: single char, \\uXXXX, 0xXX", v, attr);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         processFile (ctx, file, sink);
      }

      protected void processFile(PipelineContext ctx, String fileName, IDatasourceSink sink)
      {
         List<String> keys = new List<string>();
         sink.HandleValue(ctx, "_file/_start", fileName);
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            StreamReader rdr = new StreamReader(strm, true);
            CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, (char)0, commentChar, trim ? ValueTrimmingOptions.All : ValueTrimmingOptions.None);
            while (csvRdr.ReadNextRecord())
            {
               sink.HandleValue(ctx, "record/_start", null);
               int fieldCount = csvRdr.FieldCount;
               for (int i = keys.Count; i <= fieldCount; i++)   keys.Add(String.Format("record/f{0}", i));
               for (int i=0; i<fieldCount; i++)
               {
                  sink.HandleValue(ctx, keys[i], csvRdr[i]);
               }
               sink.HandleValue(ctx, "record", null);
            }
         }
         sink.HandleValue(ctx, "_file/_end", fileName);
      }
   }


   //public class CsvEndpoint: EndPoint
   public class CsvEndpoint : EndPoint
   {
      public readonly int CacheSize;
      public readonly int MaxParallel;
      public readonly String FileName;

      private FileStream fs;
      private StreamWriter wtr;

      char delimChar, quoteChar, commentChar;
      bool trim;




      public CsvEndpoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         FileName = engine.Xml.CombinePath (node.ReadStr("@file"));
         trim = node.OptReadBool("@trim", true);
         delimChar = CsvDatasource.readChar(node, "@dlm", ',');
         quoteChar = CsvDatasource.readChar(node, "@quote", '"');
         commentChar = CsvDatasource.readChar(node, "@comment", '#');
      }

      protected override void Open(PipelineContext ctx)
      {
         fs = new FileStream(FileName, FileMode.Create, FileAccess.Write, FileShare.Read);
         wtr = new StreamWriter (fs, Encoding.UTF8);
      }
      protected override void Close(PipelineContext ctx, bool isError)
      {
         ctx.ImportLog.Log("Closing endpoint '{0}', error={1}, flags={2}", Name, isError, ctx.ImportFlags);
         if (wtr != null)
         {
            wtr.Flush();
            wtr = null;
         }
         if (fs != null)
         {
            fs.Close();
            fs = null;
         }
      }

      protected override IDataEndpoint CreateDataEndPoint(PipelineContext ctx, string dataName)
      {
         return new CsvDataEndpoint(this);
      }

      private static int keyToIndex(string key)
      {
         String key2 = key;
         switch (key[0])
         {
            case 'f':
            case 'F':
               key2 = key.Substring(1); break;
         }

         int ret;
         if (int.TryParse(key2, NumberStyles.Integer, Invariant.Culture, out ret)) return ret;
         throw new BMException ("Fieldname '{0}' should be a number or an 'F' with a number, to make sure that the field is written on the correct place in the CSV file.", key); 
      }

      private void writeQuotedString(String txt)
      {
         if (trim) txt = txt.Trim();
         if (quoteChar == 0) wtr.Write (txt);

         wtr.Write (quoteChar);
         for (int i=0; i<txt.Length; i++)
         {
            if (txt[i]==quoteChar) wtr.Write (quoteChar);
            wtr.Write (txt[i]);
         }
         wtr.Write (quoteChar);
      }

      internal void WriteAccumulator(JObject accu)
      {
         int maxDataElts = 0;
         JToken[] data = new JToken[2*accu.Count];

         foreach (var kvp in accu)
         {
            int ix = keyToIndex (kvp.Key);
            if (ix >= data.Length)
            {
               JToken[] newArr = new JToken[2*ix+4];
               Array.Copy (data, newArr, newArr.Length);
               data = newArr;
            }
            if (ix >= maxDataElts) maxDataElts = ix+1;
            data[ix] = kvp.Value;
         }

         for (int i=0; i<maxDataElts; i++)
         {
            if (i>0) wtr.Write (this.delimChar);
            if (data[i] == null) continue;
            switch (data[i].Type)
            {
               case JTokenType.Boolean: wtr.Write ((bool)data[i]); break;
               case JTokenType.Date: wtr.Write (Invariant.ToString ((DateTime)data[i])); break;
               case JTokenType.Float: wtr.Write (Invariant.ToString ((double)data[i])); break;
               case JTokenType.Integer: wtr.Write (Invariant.ToString ((long)data[i])); break;
               case JTokenType.None:
               case JTokenType.Null:
               case JTokenType.Undefined: continue;
               default:
                  writeQuotedString((String)data[i]);
                  break;
            }
         }
         wtr.WriteLine();
      }
   }


   public class CsvDataEndpoint : JsonEndpointBase<CsvEndpoint>
   {
      public CsvDataEndpoint(CsvEndpoint endpoint)
         : base(endpoint)
      {
      }

      public override void Add(PipelineContext ctx)
      {
         if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0)
         {
            ctx.DebugLog.Log("Add: accumulator.Count={0}", accumulator.Count);
         }
         if (accumulator.Count == 0) return;
         OptLogAdd();
         EndPoint.WriteAccumulator(accumulator);
         Clear();
      }
   }

}
