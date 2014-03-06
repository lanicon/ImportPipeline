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

namespace Bitmanager.ImportPipeline
{
   public class XmlDatasource : Datasource
   {
      private String processName;
      private String uriBase;
      private IDatasourceFeeder feeder;
      private int abstractLength, abstractDelta;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         feeder = ctx.CreateFeeder(node);
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
         int lvl = -1;
         FileStream fs = null;
         try
         {
            fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 16 * 1024);
            XmlReader rdr = XmlReader.Create(fs);

            Logger l = ctx.DebugLog;
            while (rdr.Read())
            {
               l.Log("{0}: {1}, {2}", rdr.Name, rdr.NodeType, rdr.IsEmptyElement);
               switch (rdr.NodeType)
               {
                  case XmlNodeType.Element:
                     lvl++;
                     if (lvl >= keys.Count) keys.Add(null);
                     if (lvl == 0)
                        keys[0] = rdr.Name;
                     else
                        keys[lvl] = keys[lvl - 1] + "/" + rdr.Name;
                     l.Log("{0}: [{1}, {2}]", lvl, keys[lvl], rdr.NodeType);
                     bool isEmpty = rdr.IsEmptyElement;  //cache this value: after reading the attribs its value is lost 
                     if (rdr.AttributeCount > 0)
                     {
                        String pfx = keys[lvl] + "/@";
                        for (int j=0; j<rdr.AttributeCount; j++)
                        {
                           rdr.MoveToNextAttribute();
                           sink.HandleValue(ctx, pfx + rdr.Name, rdr.Value);
                        }
                     }
                     if (!isEmpty) continue;
                     l.Log("{0}: [{1}]", keys[lvl], rdr.NodeType);
                     lvl--;

                     continue;
                  case XmlNodeType.EndElement:
                     l.Log("{0}: [{1}]", keys[lvl], rdr.NodeType);
                     lvl--;
                     continue;
               }
            }
            rdr.Close();

            //sink.HandleValue(ctx, "record", null);
         }
         catch (Exception e)
         {
            if (!sink.HandleException(ctx, "record", e))
               throw;
         }
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

   }
}
