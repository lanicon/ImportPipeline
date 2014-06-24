using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.IO;
using System.Globalization;
using Bitmanager.Elastic;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline
{
   public class CsvEndpoint : Endpoint
   {
      public readonly String FileNameBase;
      private String fileName;
      public String FileName { get { return fileName; } }
      public readonly int MaxGenerations;

      private FileGenerations generations;
      private FileStream fs;
      private StreamWriter wtr;

      char delimChar, quoteChar, commentChar;
      bool trim;

      public CsvEndpoint(ImportEngine engine, XmlNode node)
         : base(node, ActiveMode.Lazy | ActiveMode.Local)
      {
         MaxGenerations = node.OptReadInt("@generations", int.MinValue);
         if (MaxGenerations != int.MinValue)
            generations = new FileGenerations();
         String tmp = MaxGenerations == int.MinValue ? node.ReadStr("@file") : node.OptReadStr("@file", null);
         FileNameBase = engine.Xml.CombinePath(tmp==null ? "csvOutput" : tmp);
         fileName = FileNameBase;
         trim = node.OptReadBool("@trim", true);
         delimChar = CsvDatasource.readChar(node, "@dlm", ',');
         quoteChar = CsvDatasource.readChar(node, "@quote", '"');
         commentChar = CsvDatasource.readChar(node, "@comment", '#');
      }

      protected override void Open(PipelineContext ctx)
      {
         if (MaxGenerations != int.MinValue)
         {
            generations.Load (Path.GetDirectoryName(FileNameBase), Path.GetFileName (FileNameBase) + "*.csv", 1);
            fileName = generations.GetGenerationName (1);
         }
         else
            fileName = FileNameBase;

         base.Open(ctx);
         fs = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read);
         wtr = new StreamWriter(fs, Encoding.UTF8, 32*1024);
      }
      protected override void Close(PipelineContext ctx)
      {
         logCloseAndCheckForNormalClose(ctx);
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
         base.Close(ctx);

         if (generations != null) generations.RemoveSuperflouisGenerations(MaxGenerations);
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName)
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
         throw new BMException("Fieldname '{0}' should be a number or an 'F' with a number, to make sure that the field is written on the correct place in the CSV file.", key);
      }

      private void writeQuotedString(String txt)
      {
         if (trim) txt = txt.Trim();
         if (quoteChar == 0) wtr.Write(txt);

         wtr.Write(quoteChar);
         for (int i = 0; i < txt.Length; i++)
         {
            if (txt[i] == quoteChar) wtr.Write(quoteChar);
            wtr.Write(txt[i]);
         }
         wtr.Write(quoteChar);
      }

      internal void WriteAccumulator(JObject accu)
      {
         int maxDataElts = 0;
         JToken[] data = new JToken[2 * accu.Count];

         foreach (var kvp in accu)
         {
            int ix = keyToIndex(kvp.Key);
            if (ix >= data.Length)
            {
               JToken[] newArr = new JToken[2 * ix + 4];
               Array.Copy(data, newArr, data.Length);
               data = newArr;
            }
            if (ix >= maxDataElts) maxDataElts = ix + 1;
            data[ix] = kvp.Value;
         }

         for (int i = 0; i < maxDataElts; i++)
         {
            if (i > 0) wtr.Write(this.delimChar);
            if (data[i] == null) continue;
            switch (data[i].Type)
            {
               case JTokenType.Boolean: wtr.Write((bool)data[i]); break;
               case JTokenType.Date: wtr.Write(Invariant.ToString((DateTime)data[i])); break;
               case JTokenType.Float: wtr.Write(Invariant.ToString((double)data[i])); break;
               case JTokenType.Integer: wtr.Write(Invariant.ToString((long)data[i])); break;
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
         Endpoint.WriteAccumulator(accumulator);
         Clear();
      }
   }

}
