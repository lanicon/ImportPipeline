using Bitmanager.Core;
using Bitmanager.Json;
using Bitmanager.IO;
using Newtonsoft.Json.Linq;

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using Bitmanager.ImportPipeline.StreamProviders;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public class TextDatasource : StreamDatasourceBase
   {
      public TextDatasource() : base(true, true) { }

      private enum _Mode {lines=1, values=2, stopAtEmpty=4};
      private int maxToRead;
      private _Mode mode;
      private bool lenient;

      public override void Init(PipelineContext ctx, XmlNode node)
      {
         base.Init(ctx, node, Encoding.Default);
         maxToRead = node.ReadInt("@maxread", int.MaxValue);
         mode = node.ReadEnum<_Mode>("@mode", _Mode.values);
         lenient = node.ReadBool("@lenient", false); 
      }

      protected override void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm)
      {
         int lineNo = -1;
         try
         {
            TextReader rdr = strm.CreateTextReader(encoding);

            int charsRead = 0;
            if ((mode & _Mode.lines) != 0)
            {
               while (charsRead < maxToRead)
               {
                  lineNo++;
                  String line = rdr.ReadLine();
                  if (line == null) break;
                  if (line.Length == 0)
                  {
                     if ((mode & _Mode.stopAtEmpty) != 0) break;
                  }
                  sink.HandleValue(ctx, "record/line", line);
                  charsRead += line.Length;
               }
            }
            else
            {
               lineNo++;
               String line = rdr.ReadLine();
               if (line != null) charsRead += line.Length;
               String key, value;
               while (line != null)
               {
                  lineNo++;
                  String nextLine = rdr.ReadLine();
                  if (nextLine == null)
                  {
                     key = "record/" + splitKV(line, out value);
                     sink.HandleValue(ctx, key, value);
                     break;
                  }
                  charsRead += nextLine.Length;
                  if (nextLine.Length == 0)
                  {
                     if ((mode & _Mode.stopAtEmpty) != 0) break; else continue;
                  }

                  int offs = 0;
                  for (; offs < nextLine.Length; offs++)
                  {
                     switch (nextLine[offs])
                     {
                        case ' ':
                        case '\t': continue;
                     }
                     break;
                  }

                  if (offs > 0)
                  {
                     line = line + nextLine.Substring(offs);
                     continue;
                  }

                  if (lenient && nextLine.IndexOf(':') < 0)
                  {
                     line = line + nextLine;
                     continue;
                  }

                  key = "record/" + splitKV(line, out value);
                  sink.HandleValue(ctx, key, value);
                  line = nextLine;
               }
            }
            sink.HandleValue(ctx, "record", null);
            ctx.IncrementEmitted();
         }
         catch (Exception e)
         {
            e = new BMException(e, "{0}\nLine={1}.", e.Message, lineNo);
            ctx.HandleException(e);
         }
      }


      private String splitKV (String line, out string value)
      {
         int i = line.IndexOf(':');
         if (i<0) throw new BMException ("Unexpected key/value line: missing ':'.");
         int j = i + 1;
         for (; j < line.Length; j++)
            if (line[i] != ' ') break;
         value = line.Substring(j);
         return line.Substring(0, i);
      }
   }
}
