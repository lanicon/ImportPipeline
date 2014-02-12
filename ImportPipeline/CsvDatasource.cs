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
         sink.HandleValue(ctx, Pipeline.ItemStart, fileName);
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            StreamReader rdr = new StreamReader(strm, true);
            //CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, (char)0, commentChar, trim ? ValueTrimmingOptions.UnquotedOnly : ValueTrimmingOptions.None);
            //CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, quoteChar, commentChar, trim ? ValueTrimmingOptions.UnquotedOnly : ValueTrimmingOptions.None); //, trim, 4096);
            CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, quoteChar, commentChar, trim);
            Logs.ErrorLog.Log("Multiline={0}, quote={1} ({2}), esc={3} ({4})", csvRdr.SupportsMultiline, csvRdr.Quote, (int)csvRdr.Quote, csvRdr.Escape, (int)csvRdr.Escape);
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
         sink.HandleValue(ctx, Pipeline.ItemStop, fileName);
      }
   }


}
