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
      int[] sortValuesToKeep;
      int sortKey;
      int startAt;


      char delimChar, quoteChar, commentChar;
      bool hasHeaders;
      ValueTrimmingOptions trim;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         file = ctx.ImportEngine.Xml.CombinePath (node.ReadStr("@file"));
         hasHeaders = node.OptReadBool("@headers", false);
         trim = node.OptReadEnum ("@trim", ValueTrimmingOptions.UnquotedOnly);
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
         if (sortKey < 0) processFile(ctx, file, sink);
         else processSortedFile(ctx, file, sink);
      }

      protected void processFile(PipelineContext ctx, String fileName, IDatasourceSink sink)
      {
         List<String> keys = new List<string>();
         sink.HandleValue(ctx, Pipeline.ItemStart, fileName);
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            StreamReader rdr = new StreamReader(strm, true);
            //CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, (char)0, commentChar, trim ? ValueTrimmingOptions.UnquotedOnly : ValueTrimmingOptions.None);
            //CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, quoteChar, commentChar, trim ? ValueTrimmingOptions.UnquotedOnly : ValueTrimmingOptions.None); //, trim, 4096);
            CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, quoteChar, commentChar, trim);
            Logs.ErrorLog.Log("Multiline={0}, quote={1} ({2}), esc={3} ({4}), startat={5}", csvRdr.SupportsMultiline, csvRdr.Quote, (int)csvRdr.Quote, csvRdr.Escape, (int)csvRdr.Escape, startAt);
            int line;
            for (line=0; csvRdr.ReadNextRecord(); line++ )
            {
               if (startAt > line) continue;
               sink.HandleValue(ctx, "record/_start", null);
               int fieldCount = csvRdr.FieldCount;
               ctx.DebugLog.Log("Record {0}. FC={1}", line, fieldCount); 
               for (int i = keys.Count; i <= fieldCount; i++) keys.Add(String.Format("record/f{0}", i));
               for (int i = 0; i < fieldCount; i++)
               {
                  sink.HandleValue(ctx, keys[i], csvRdr[i]);
               }
               sink.HandleValue(ctx, "record", null);
            }
         }
         sink.HandleValue(ctx, Pipeline.ItemStop, fileName);
      }


      private int cbSortString(String[] a, String[] b)
      {
         return StringComparer.OrdinalIgnoreCase.Compare(a[0], b[0]);
      }
      protected void processSortedFile(PipelineContext ctx, String fileName, IDatasourceSink sink)
      {
         List<String[]> rows = new List<string[]>();

         int maxFieldCount = 0;
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            StreamReader rdr = new StreamReader(strm, true);
            CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, quoteChar, commentChar, trim);
            while (csvRdr.ReadNextRecord())
            {
               int fieldCount = csvRdr.FieldCount;
               if (fieldCount > maxFieldCount) maxFieldCount = fieldCount;
               String[] arr = new String[fieldCount+1];

               for (int i = 0; i < fieldCount; i++) arr[i+1] = csvRdr[i];
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
            sink.HandleValue(ctx, "record", null);
         }
         sink.HandleValue(ctx, Pipeline.ItemStop, fileName);
      }
   }

#if true
   public class _Reader
   {
      private StreamReader reader;
      private int line;
      private int nextChar;
      private int quoteChar;
      private int sepChar;
      private int escapeChar;
      private List<String> fields;
      public List<String> Fields { get { return fields; } }
      public int Line { get { return line; } }
      public String SepChar
      {
         get { return new String((char)sepChar, 1); }
         set { sepChar = String.IsNullOrEmpty(value) ? -1 : (int)value[0]; }
      }
      public String QuoteChar
      {
         get { return quoteChar < 0 ? null : new String((char)quoteChar, 1); }
         set { quoteChar = String.IsNullOrEmpty(value) ? -1 : (int)value[0]; }
      }
      public String EscapeChar
      {
         get { return escapeChar < 0 ? null : new String((char)escapeChar, 1); }
         set { escapeChar = String.IsNullOrEmpty(value) ? -1 : (int)value[0]; }
      }

      public int SepOrd
      {
         get { return sepChar; }
         set { sepChar = value; }
      }
      public int QuoteOrd
      {
         get { return quoteChar; }
         set { quoteChar = value; }
      }
      public int EscapeOrd
      {
         get { return escapeChar; }
         set { escapeChar = value; }
      }

      //public _Reader (using (StreamReader sr = new StreamReader("TestFile.txt")) 
      public _Reader(StreamReader sr)
      {
         reader = sr;
         nextChar = sr.Read();
         fields = new List<string>();
         quoteChar = '"';
         sepChar = 9;
         escapeChar = -1;
         line = -1;
      }
      public _Reader(Stream strm): this (new StreamReader (strm, Encoding.UTF8, true, 4096))
      {
      }

      enum _State { None=0, InNormalField=1, InQuotedField=2 };

      private void addFieldAndClear(StringBuilder sb)
      {
         if (sb.Length == 0)
         {
            fields.Add(String.Empty);
            return;
         }
         fields.Add(sb.ToString());
         sb.Length = 0;
      }

      public bool NextRecord()
      {
         fields.Clear();
         if (nextChar < 0) return false;
         ++line;
         int ch = nextChar;
         
         _State state = _State.None;
         StringBuilder sb = new StringBuilder();
         int pos = 0;

         while (true)
         {
            switch (ch)
            {
               case -1:
                  if (fields.Count > 0 || sb.Length > 0) addFieldAndClear(sb);
                  goto EOR;

               case '\r':
               case '\n':
                  if (state == _State.InQuotedField)
                  {
                     sb.Append((char)ch);
                     goto NEXT_CHAR;
                  }
                  if (fields.Count > 0 || sb.Length>0) addFieldAndClear (sb);
                  if (ch == '\r')
                  {
                     ch = reader.Read();
                     if (ch != '\n') goto EOR;
                  }
                  else
                  {
                     ch = reader.Read();
                     if (ch != '\r') goto EOR;
                  }
                  ch = reader.Read();
                  goto EOR;
            }

            if (ch == sepChar)
            {
               if (state == _State.InQuotedField)
               {
                  sb.Append((char)ch);
                  goto NEXT_CHAR;
               }
               addFieldAndClear(sb);
               state = _State.InNormalField;
               goto NEXT_CHAR;
            }

            if (ch == quoteChar)
            {
               if (state == _State.InQuotedField)
               {
                  ch = reader.Read();
                  if (ch == quoteChar)
                  {
                     sb.Append((char)ch);
                     goto NEXT_CHAR;
                  }

                  while (ch == ' ') ch = reader.Read();
                  switch (ch)
                  {
                     default:
                        if (ch == sepChar) break;
                        nextChar = -1;
                        throw new BMException("error at line {0}: unexpected char {1}, field={2}. ", line, (char)ch, fields.Count);
                     case '\r':
                     case '\n':
                     case -1:
                        break;
                  }
                  state = _State.None;
                  continue;
               }

               //Should be the beginning of a quoted field...
               for (int i=0; i<sb.Length; i++)
                  if (sb[i] != ' ')
                  {
                     nextChar = -1;
                     throw new BMException("error at line {0}: unexpected char {1}, field={2}.", line, (char)ch, fields.Count);
                  }
               sb.Length = 0;
               state = _State.InQuotedField;
               goto NEXT_CHAR;
            }

            sb.Append ((char) ch);


         NEXT_CHAR:
            ch=reader.Read();
         }
         EOR:
         nextChar = ch;
         return true;
      }
   }
#endif

}
