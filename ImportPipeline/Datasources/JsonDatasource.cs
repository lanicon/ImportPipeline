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
using Bitmanager.ImportPipeline.StreamProviders;

namespace Bitmanager.ImportPipeline
{
   public class JsonDatasource : Datasource
   {
      //PW private IDatasourceFeeder feeder;
      private GenericStreamProvider streamProvider;
      private int splitUntil;
      private bool dumpReader;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         //PW feeder = ctx.CreateFeeder(node);
         streamProvider = new GenericStreamProvider(ctx, node);
         dumpReader = node.ReadBool("@debug", false);
         splitUntil = node.ReadInt("@splituntil", 1);
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
      private void importUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         StringDict attribs = getAttributes(elt.ContextNode);
         var fullElt = elt; //pw  (FileNameFeederElement)elt;
         String fileName = fullElt.FullName;
         sink.HandleValue(ctx, "_start", fileName);
         //DateTime dtFile = File.GetLastWriteTimeUtc(fileName);
         //sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         sink.HandleValue(ctx, "record/virtualFilename", fullElt.FullName); //pwVirtualFileName);

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

         List<String> keys = new List<string>();
         List<String> values = new List<String>();
         Stream fs = null;
         try
         {
            fs = elt.CreateStream(); //PW new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 16 * 1024);
            JsonTextReader rdr = new JsonTextReader  (new StreamReader (fs, true));
            JObject obj = (JObject)JObject.ReadFrom(rdr);
            rdr.Close();
            fs.Close();

            Pipeline.EmitToken (ctx, sink, obj, "record", splitUntil);
            ctx.IncrementEmitted();
         }
         catch (Exception e)
         {
            ctx.HandleException(e);
         }
      }

      public static String WrapMessage (Exception ex, String sub, String fmt)
      {
         String msg = ex.Message;
         if (msg.IndexOf(sub) >= 0) return msg;
         return String.Format(fmt, msg, sub);
      }
      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         //PW
         //foreach (var elt in feeder.GetElements(ctx))
         //{
         //   try
         //   {
         //      importUrl(ctx, sink, elt);
         //   }
         //   catch (Exception e)
         //   {
         //      throw new BMException(e, e.Message + "\r\nUrl=" + elt.Element + ".");
         //   }
         //}
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
