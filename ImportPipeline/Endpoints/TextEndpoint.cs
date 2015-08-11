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
   public class TextEndpoint : Endpoint
   {
      public readonly String FileNameBase;
      private String fileName;
      public String FileName { get { return fileName; } }
      public readonly int MaxGenerations;
      public readonly String Header, Footer, Format;
      public readonly Encoding Encoding;

      private TextWriter textWriter;
      private FileGenerations generations;
      private String[] fields;
      private Object[] formatParms;
      private bool rawTokens;

      public TextEndpoint(ImportEngine engine, XmlNode node)
         : base(engine, node, ActiveMode.Lazy | ActiveMode.Local)
      {
         MaxGenerations = node.ReadInt("@generations", int.MinValue);
         if (MaxGenerations != int.MinValue)
            generations = new FileGenerations();
         String tmp = MaxGenerations == int.MinValue ? node.ReadStr("@file") : node.ReadStr("@file", null);
         FileNameBase = engine.Xml.CombinePath(tmp == null ? "textOutput" : tmp);
         fileName = FileNameBase;

         rawTokens = node.ReadBool("@rawtokens", false);

         Format = node.ReadStr("@format", null);
         if (Format == null) Format = node.ReadStr("format");
         if (Format == "*") Format = null;
         Header = node.ReadStr("@header", null);
         if (Header == null) Header = node.ReadStr("header", null);
         Footer = node.ReadStr("@footer", null);
         if (Footer == null) Footer = node.ReadStr("footer", null);
         
         String cs = node.ReadStr("@encoding", null);
         Encoding = cs == null ? Encoding.Default : Encoding.GetEncoding(cs);

         //Predefine field orders if requested (implies linient mode)
         fields = node.ReadStr("@fieldorder", null).SplitStandard();
         if (fields != null && fields.Length > 0)
            formatParms = new Object[fields.Length];
         else
            fields = null;
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
         FileStream fs = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read, 4096, FileOptions.SequentialScan);
         textWriter = fs.CreateTextWriter(Encoding);
         if (Header != null)
            textWriter.WriteLine(Header);
      }

      protected override void Close(PipelineContext ctx)
      {
         logCloseAndCheckForNormalClose(ctx);
         if (Footer != null && textWriter != null)
            textWriter.WriteLine(Footer);
         Utils.FreeAndNil(ref textWriter);
         base.Close(ctx);
         if (generations != null) generations.RemoveSuperflouisGenerations(MaxGenerations);
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName, bool mustExcept)
      {
         return new TextDataEndpoint(this);
      }


      private String[] createFieldIndexes(JObject accu)
      {
         var fields = new String[accu.Count];
         int i = 0;
         foreach (var kvp in accu)
         {
            fields[i] = kvp.Key;
            i++;
         }
         Array.Sort(fields, StringComparer.OrdinalIgnoreCase);
         formatParms = new Object[accu.Count];
         this.fields = fields;
         return fields;
      }
      internal void WriteAccumulator(JObject accu)
      {
         if (Format==null)
         {
            textWriter.WriteLine(accu.ToString());
            return;
         }
         var indexes = fields;
         if (indexes == null) indexes = createFieldIndexes(accu);

         for (int i = 0; i < indexes.Length; i++)
         {
            JToken tk = accu[indexes[i]];
            if (tk==null)
            {
               formatParms[i] = null;
               continue;
            }
            formatParms[i] = rawTokens ? tk : tk.ToNative(); 
         }
         textWriter.WriteLine (Invariant.Format (Format, formatParms));
      }




      public class TextDataEndpoint : JsonEndpointBase<TextEndpoint>
      {
         public TextDataEndpoint(TextEndpoint endpoint)
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



}
