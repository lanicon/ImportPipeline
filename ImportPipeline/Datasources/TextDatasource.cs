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
   public class TextDatasource : Datasource
   {
      private enum _Mode {lines=1, values=2, stopAtEmpty=4};
      private GenericStreamProvider streamProvider;
      private Encoding encoding;
      private int maxToRead;
      private _Mode mode;
      private bool lenient;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         streamProvider = new GenericStreamProvider(ctx, node);
         maxToRead = node.ReadInt("@maxread", int.MaxValue);
         String enc = node.ReadStr("@encoding", null);
         encoding = enc == null ? Encoding.Default : Encoding.GetEncoding(enc);
         mode = node.ReadEnum<_Mode>("@mode", _Mode.values);
         lenient = node.ReadBool("@lenient", false); 
      }


      private static ExistState toExistState(Object result)
      {
         if (result == null || !(result is ExistState)) return ExistState.NotExist;
         return (ExistState)result;
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         int lineNo = -1;
         var fullElt = elt;
         String fileName = fullElt.FullName;
         sink.HandleValue(ctx, "_start", fileName);
         //DateTime dtFile = File.GetLastWriteTimeUtc(fileName);
         //sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         sink.HandleValue(ctx, "record/filename", fullElt.FullName); 

         ExistState existState = ExistState.NotExist;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            existState = toExistState(sink.HandleValue(ctx, "record/_checkexist", null));
         }

         //Check if we need to convert this file
         if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
         {
            ctx.Skipped++;
            ctx.ImportLog.Log("Skipped: {0}. Date={1}", elt, 0);// dtFile);
            return;
         }

         Stream fs = null;
         try
         {
            fs = elt.CreateStream(); 
            TextReader rdr = fs.CreateTextReader (encoding);

            int charsRead = 0;
            if ((mode & _Mode.lines) != 0)
            {
               while (charsRead < maxToRead)
               {
                  lineNo++;
                  String line = rdr.ReadLine();
                  if (line==null) break;
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
                  if (nextLine==null)
                  {
                     key = "record/" + splitKV (line, out value);
                     sink.HandleValue(ctx, key, value);
                     break;
                  }
                  charsRead += nextLine.Length;
                  if (nextLine.Length == 0)
                  {
                     if ((mode & _Mode.stopAtEmpty) != 0) break; else continue ;
                  }

                  int offs = 0;
                  for (; offs<nextLine.Length; offs++)
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
                     line = line + nextLine.Substring (offs);
                     continue;
                  }

                  if (lenient && nextLine.IndexOf(':') < 0)
                  {
                     line = line + nextLine;
                     continue;
                  }

                  key = "record/" + splitKV (line, out value);
                  sink.HandleValue(ctx, key, value);
                  line = nextLine;
               }
            }

            sink.HandleValue(ctx, "record", null);
            rdr.Close();
            fs.Close();

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

      public static String WrapMessage (Exception ex, String sub, String fmt)
      {
         String msg = ex.Message;
         if (msg.IndexOf(sub) >= 0) return msg;
         return String.Format(fmt, msg, sub);
      }
      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in streamProvider.GetElements(ctx))
         {
            try
            {
               importUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, WrapMessage (e, elt.ToString(), "{0}\r\nUrl={1}."));
            }
         }
      }

   }
}
