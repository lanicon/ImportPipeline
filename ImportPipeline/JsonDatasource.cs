using Bitmanager.ImportPipeline;
using Bitmanager.Core;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using System.Threading.Tasks;
using System.Xml;
using System.Net;
using System.IO;
using HtmlAgilityPack;
using System.Text.RegularExpressions;
using System.Web;
using System.Threading;
using Bitmanager.Elastic;
using Newtonsoft.Json;

namespace Bitmanager.ImportPipeline
{
   public class JsonDatasource : Datasource
   {
      private IDatasourceFeeder feeder;
      private bool dumpReader;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         feeder = ctx.CreateFeeder(node);
         dumpReader = node.ReadBool("@debug", false);
      }


      private StringDict getAttributes(XmlNode node)
      {
         StringDict ret = new StringDict();
         if (node == null) return ret;
         var coll = node.Attributes;
         for (int i = 0; i < coll.Count; i++)
         {
            var att = coll[i];
            if (att.LocalName.Equals("url", StringComparison.InvariantCultureIgnoreCase)) continue;
            if (att.LocalName.Equals("baseurl", StringComparison.InvariantCultureIgnoreCase)) continue;
            ret[att.LocalName] = att.Value;
         }
         return ret;
      }

      private static ExistState toExistState(Object result)
      {
         if (result == null || !(result is ExistState)) return ExistState.NotExist;
         return (ExistState)result;
      }
      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IDatasourceFeederElement elt)
      {
         StringDict attribs = getAttributes(elt.Context);
         var fullElt = (FileNameFeederElement)elt;
         String fileName = fullElt.FileName;
         sink.HandleValue(ctx, "_start", fileName);
         DateTime dtFile = File.GetLastWriteTimeUtc(fileName);
         sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         sink.HandleValue(ctx, "record/virtualFilename", fullElt.VirtualFileName);

         ExistState existState = ExistState.NotExist;
         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            existState = toExistState(sink.HandleValue(ctx, "record/_checkexist", null));
         }

         //Check if we need to convert this file
         if ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) != 0)
         {
            ctx.Skipped++;
            ctx.ImportLog.Log("Skipped: {0}. Date={1}", fullElt.VirtualFileName, dtFile);
            return;
         }

         List<String> keys = new List<string>();
         List<String> values = new List<String>();
         int lvl = -1;
         FileStream fs = null;
         try
         {
            fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 16 * 1024);
            JsonTextReader rdr = new JsonTextReader  (new StreamReader (fs, true));
            JObject obj = (JObject)JObject.ReadFrom(rdr);
            rdr.Close();
            fs.Close();

            Pipeline.EmitToken (ctx, sink, obj, "record", -1);
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in feeder.GetElements(ctx))
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

   }
}
