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

namespace Bitmanager.ImportPipeline
{
   public class CsvDatasource: Datasource
   {
      private IDatasourceFeeder feeder;
      //DSString file;
      int[] sortValuesToKeep;
      int sortKey;
      int startAt;


      char delimChar, quoteChar, commentChar;
      bool hasHeaders;
      CsvTrimOptions trim;


      public void Init(PipelineContext ctx, XmlNode node)
      {
         feeder = ctx.CreateFeeder(node, typeof (FileNameFeeder));
         //DS file = ctx.ImportEngine.Xml.CombinePath(node.ReadStr("@file"));
         hasHeaders = node.OptReadBool("@headers", false);
         trim = node.OptReadEnum("@trim", CsvTrimOptions.None);
         delimChar = readChar(node, "@dlm", ',');
         quoteChar = readChar(node, "@quote", '"');
         commentChar = readChar(node, "@comment", '#');
         startAt = node.OptReadInt("@startat", -1);

         String sort = node.OptReadStr("@sort", null);
         sortKey = -1;
         if (sort != null)
         {
            sortKey = interpretField(sort);
         }
      }

      private static int interpretField(String x)
      {
         switch (x[0])
         {
            case 'f':
            case 'F': x = x.Substring(1); break;
         }
         return Invariant.ToInt32(x);
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
         ctx.Emitted = 0;
         foreach (var elt in feeder)
         {
            try
            {
               String file = elt.Element.ToString();
               ctx.ImportLog.Log("-- Importing {0}...", file);
               if (sortKey < 0) processFile(ctx, file, sink);
               else processSortedFile(ctx, file, sink);
            }
            catch (Exception e)
            {
               throw new BMException(e, e.Message + "\r\nFile=" + elt.Element + ".");
            }
         }
      }

      protected void processFile(PipelineContext ctx, String fileName, IDatasourceSink sink)
      {
         List<String> keys = new List<string>();
         sink.HandleValue(ctx, Pipeline.ItemStart, fileName);
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            CsvReader csvRdr = createReader(strm);
            int line;
            for (line=0; csvRdr.NextRecord(); line++ )
            {
               if (startAt > line) continue;
               sink.HandleValue(ctx, "record/_start", null);
               var fields = csvRdr.Fields;
               int fieldCount = fields.Count;
               //ctx.DebugLog.Log("Record {0}. FC={1}", line, fieldCount); 
               for (int i = keys.Count; i <= fieldCount; i++) keys.Add(String.Format("record/f{0}", i));
               for (int i = 0; i < fieldCount; i++)
               {
                  sink.HandleValue(ctx, keys[i], fields[i]);
               }
               ctx.Emitted++;
               sink.HandleValue(ctx, "record", null);
            }
         }
         sink.HandleValue(ctx, Pipeline.ItemStop, fileName);
      }

      private int cbSortString(String[] a, String[] b)
      {
         return StringComparer.OrdinalIgnoreCase.Compare(a[0], b[0]);
      }
      CsvReader createReader(FileStream strm)
      {
         CsvReader rdr = new CsvReader(strm);
         rdr.QuoteOrd = (int)quoteChar;
         rdr.SepOrd = (int)delimChar;
         rdr.SkipHeader = hasHeaders;
         rdr.SkipEmptyRecords = true;
         rdr.TrimOptions = trim;
         //Logs.ErrorLog.Log("Multiline={0}, quote={1} ({2}), esc={3} ({4}), startat={5}", true, csvRdr.QuoteChar, (int)csvRdr.QuoteOrd, csvRdr.EscapeOrd, (int)csvRdr.SepOrd, startAt);
         return rdr;
      }
      protected void processSortedFile(PipelineContext ctx, String fileName, IDatasourceSink sink)
      {
         List<String[]> rows = new List<string[]>();

         int maxFieldCount = 0;
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            CsvReader csvRdr = createReader(strm);
            while (csvRdr.NextRecord())
            {
               var fields = csvRdr.Fields;
               int fieldCount = fields.Count;
               if (fieldCount > maxFieldCount) maxFieldCount = fieldCount;
               String[] arr = new String[fieldCount+1];

               for (int i = 0; i < fieldCount; i++) arr[i+1] = fields[i];
               if (fieldCount > sortKey) arr[0] = arr[sortKey+1];
               rows.Add (arr);
            }
         }

         ctx.DebugLog.Log("First 10 sortkeys:");
         int N = rows.Count;
         if (N > 10) N = 10;
         for (int i = 0; i < N; i++)
         {
            ctx.DebugLog.Log("-- [{0}]: '{1}'", i, rows[i][0]);
         }

         rows.Sort (cbSortString);

         ctx.DebugLog.Log("First 10 sortkeys after sort:");
         for (int i = 0; i < N; i++)
         {
            ctx.DebugLog.Log("-- [{0}]: '{1}'", i, rows[i][0]);
         }

         //Fill pre-calculated keys
         List<String> keys = new List<string>();
         for (int i = 0; i <= maxFieldCount; i++) keys.Add(String.Format("record/f{0}", i));

         //Emit sorted records
         sink.HandleValue(ctx, Pipeline.ItemStart, fileName);
         for (int r = 0; r < rows.Count; r++)
         {
            String[] arr = rows[r];
            rows[r] = null; //Let this element be GC-ed
            sink.HandleValue(ctx, "record/_start", null);
            for (int i = 1; i < arr.Length; i++) //arr[0] is the sortkey
            {
               sink.HandleValue(ctx, keys[i-1], arr[i]);
            }
            ctx.Emitted++;
            sink.HandleValue(ctx, "record", null);
         }
         sink.HandleValue(ctx, Pipeline.ItemStop, fileName);
      }
   }

}
