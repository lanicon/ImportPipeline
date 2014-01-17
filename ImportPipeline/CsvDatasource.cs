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

namespace Bitmanager.ImportPipeline
{
   public class CsvDatasource: Datasource
   {
      String file;
      char delimChar, quoteChar, commentChar;
      bool hasHeaders, trim;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         file = node.ReadStr("@file");
         hasHeaders = node.OptReadBool("@headers", false);
         trim = node.OptReadBool("@trim", true);
         delimChar = readChar(node, "@dlm", ',');
         quoteChar = readChar(node, "@quote", '"');
         commentChar = readChar(node, "@comment", '#');
      }

      private char readChar(XmlNode node, String attr, char def)
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
         sink.HandleValue(ctx, "file_start", fileName);
         using (FileStream strm = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
         {
            StreamReader rdr = new StreamReader(strm, true);
            CsvReader csvRdr = new CsvReader(rdr, hasHeaders, delimChar, quoteChar, (char)0, commentChar, trim ? ValueTrimmingOptions.All : ValueTrimmingOptions.None);
            while (csvRdr.ReadNextRecord())
            {
               sink.HandleValue(ctx, "file/record_start", null);
               int fieldCount = csvRdr.FieldCount;
               for (int i=0; i<fieldCount; i++)
               {
                  sink.HandleValue(ctx, String.Format ("file/record/f{0}", i), csvRdr[i]);
               }
               sink.HandleValue(ctx, "file/record_end", null);
            }
         }
         sink.HandleValue(ctx, "file_end", fileName);
      }
   }
}
