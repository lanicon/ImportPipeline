using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using Bitmanager.Elastic;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System.Xml;


namespace Bitmanager.ImportPipeline
{
   public class ESDatasource : Datasource
   {
      private IDatasourceFeeder feeder;
      private String timeout;
      private int numRecords;

      public void Init(PipelineContext ctx, XmlNode node)
      {
         feeder = ctx.CreateFeeder(node, typeof (UrlFeeder));
         numRecords = node.OptReadInt("@buffersize", ESRecordEnum.DEF_BUFFER_SIZE);
         timeout = node.OptReadStr("@timeout", ESRecordEnum.DEF_TIMEOUT);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in feeder)
         {
            try
            {
               importUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, e.Message + "\r\nUrl=" + elt.Element + ".");
            }
         }
      }

      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IDatasourceFeederElement elt)
      {
         //StringDict attribs = getAttributes(elt.Context);
         //var fullElt = (FileNameFeederElement)elt;
         String url = elt.ToString();
         sink.HandleValue(ctx, "record/_start", url);
         String index = elt.Context.ReadStr ("@index");

         //ExistState existState = ExistState.NotExist;
         //if ((ctx.Flags & _ImportFlags.ImportFull) == 0) //Not a full import
         //{
         //   existState = toExistState(sink.HandleValue(ctx, "record/_checkexist", null));
         //}

         ////Check if we need to convert this file
         //if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
         //{
         //   ctx.Skipped++;
         //   ctx.ImportLog.Log("Skipped: {0}. Date={1}", fullElt.VirtualFileName, dtFile);
         //   return;
         //}

         try
         {
            Uri uri = new Uri (url);
            ESConnection conn = new ESConnection (url);
            ESRecordEnum e = new ESRecordEnum(conn, index, null, numRecords, timeout);
            foreach (var doc in e)
            {
               String[] fields = doc.GetLoadedFields();
               for (int i = 0; i < fields.Length; i++)
               {
                  String field = fields[i];
                  sink.HandleValue(ctx, "record/" + field, doc.GetFieldAsToken(field));
               }
               sink.HandleValue(ctx, "record", null);
            }
         }
         catch (Exception e)
         {
            if (!sink.HandleException(ctx, "record", e))
               throw;
         }
      }
   }
}
