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
      private CsvWriter csvWtr;
      private StringDict<int> lenientIndexes;

      char delimChar, quoteChar, commentChar;
      bool trim, lenient;

      public CsvEndpoint(ImportEngine engine, XmlNode node)
         : base(node, ActiveMode.Lazy | ActiveMode.Local)
      {
         MaxGenerations = node.ReadInt("@generations", int.MinValue);
         if (MaxGenerations != int.MinValue)
            generations = new FileGenerations();
         String tmp = MaxGenerations == int.MinValue ? node.ReadStr("@file") : node.ReadStr("@file", null);
         FileNameBase = engine.Xml.CombinePath(tmp == null ? "csvOutput" : tmp);
         fileName = FileNameBase;
         trim = node.ReadBool("@trim", true);
         lenient = node.ReadBool("@lenient", false);
         delimChar = CsvDatasource.readChar(node, "@dlm", ',');
         quoteChar = CsvDatasource.readChar(node, "@quote", '"');
         commentChar = CsvDatasource.readChar(node, "@comment", '#');

         //Predefine field orders if requested (implies linient mode)
         String[] fieldOrder = node.ReadStr("@fieldorder", null).SplitStandard();
         if (fieldOrder != null)
         {
            lenient = true;
            foreach (var fld in fieldOrder) keyToIndex(fld);
         }
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
         csvWtr = new CsvWriter(fileName);
         csvWtr.QuoteOrd = quoteChar;
         csvWtr.SepOrd = delimChar;
      }

      protected override void Close(PipelineContext ctx)
      {
         logCloseAndCheckForNormalClose(ctx);
         Utils.FreeAndNil(ref csvWtr);
         base.Close(ctx);
         if (lenient)
         {
            ctx.ImportLog.Log("Lenient indexes: '{0}'", getLenientIndexes());
         }
         if (generations != null) generations.RemoveSuperflouisGenerations(MaxGenerations);
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName, bool mustExcept)
      {
         return new CsvDataEndpoint(this);
      }

      private String getLenientIndexes()
      {
         if (lenientIndexes == null) return null;
         StringBuilder sb = new StringBuilder();
         List<KeyValuePair<String, int>> list = new List<KeyValuePair<string,int>>();
         foreach (var kvp in lenientIndexes) list.Add(kvp);
         list.Sort((x, y) => Comparer<int>.Default.Compare(x.Value, y.Value));

         for (int i = 0; i < list.Count; i++)
         {
            if (i > 0) sb.Append(this.delimChar);
            sb.Append(list[i].Key);
         }
         return sb.ToString();
      }

      private int keyToIndex(string key)
      {
         int ret;
         if (lenient)
         {
            if (lenientIndexes == null) lenientIndexes = new StringDict<int>(false);
            if (lenientIndexes.TryGetValue(key, out ret)) return ret;
            ret = lenientIndexes.Count;
            lenientIndexes.Add(key, ret);
            return ret;
         }
         String key2 = key;
         switch (key[0])
         {
            case 'f':
            case 'F':
               key2 = key.Substring(1); break;
         }

         if (int.TryParse(key2, NumberStyles.Integer, Invariant.Culture, out ret)) return ret;
         throw new BMException("Fieldname '{0}' should be a number or an 'F' with a number, to make sure that the field is written on the correct place in the CSV file.", key);
      }

      internal void WriteAccumulator(JObject accu)
      {
         foreach (var kvp in accu)
         {
            int ix = keyToIndex(kvp.Key);
            JToken v = kvp.Value;
            if (v == null) continue;

            switch (v.Type)
            {
               case JTokenType.Boolean: csvWtr.SetField(ix, (bool)v); break;
               case JTokenType.Date: csvWtr.SetField(ix, (DateTime)v); break;
               case JTokenType.Float: csvWtr.SetField(ix, (double)v); break;
               case JTokenType.Integer: csvWtr.SetField(ix, (long)v); break;
               case JTokenType.None:
               case JTokenType.Null:
               case JTokenType.Undefined: continue;
               default: csvWtr.SetField(ix, v.ToString()); break;
            }
         }
         csvWtr.WriteRecord();
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
