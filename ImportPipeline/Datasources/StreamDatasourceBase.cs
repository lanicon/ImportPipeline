using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using Bitmanager.Core;
using Bitmanager.Json;
using Bitmanager.IO;
using Bitmanager.Xml;
using Bitmanager.ImportPipeline.StreamProviders;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public abstract class StreamDatasourceBase : Datasource
   {
      protected GenericStreamProvider streamProvider;
      protected Encoding encoding;


      public virtual void Init(PipelineContext ctx, XmlNode node)
      {
         Init(ctx, node, Encoding.UTF8);
      }
      public virtual void Init(PipelineContext ctx, XmlNode node, Encoding defEncoding)
      {
         streamProvider = new GenericStreamProvider(ctx, node);
         String enc = node.ReadStr("@encoding", null);
         encoding = enc == null ? defEncoding : Encoding.GetEncoding(enc);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         foreach (var elt in streamProvider.GetElements(ctx))
         {
            try
            {
               ImportUrl(ctx, sink, elt);
            }
            catch (Exception e)
            {
               e = new BMException(e, WrapMessage(e, elt.ToString(), "{0}\r\nUrl={1}."));
               ctx.HandleException(e);
            }
         }
      }

      public static String WrapMessage(Exception ex, String sub, String fmt)
      {
         String msg = ex.Message;
         if (msg.IndexOf(sub) >= 0) return msg;
         return String.Format(fmt, msg, sub);
      }

      protected static ExistState toExistState(Object result)
      {
         if (result == null || !(result is ExistState)) return ExistState.NotExist;
         return (ExistState)result;
      }

      protected virtual bool CheckNeedImport (PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         ExistState existState = toExistState(sink.HandleValue(ctx, "record/_checkexist", null));

         //return true if we need to convert this file
         return ((existState & (ExistState.ExistSame | ExistState.ExistNewer | ExistState.Exist)) == 0);
      }

      protected abstract void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm);

      protected virtual void ImportUrl(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt)
      {
         sink.HandleValue(ctx, "_start", elt.FullName);
         DateTime dtFile = elt.LastModified;
         sink.HandleValue(ctx, "record/lastmodutc", dtFile);
         sink.HandleValue(ctx, "record/filename", elt.FullName);
         sink.HandleValue(ctx, "record/virtualfilename", elt.VirtualName);

         if ((ctx.ImportFlags & _ImportFlags.ImportFull) == 0) //Not a full import
         {
            if (CheckNeedImport(ctx, sink, elt))
            {
               ctx.Skipped++;
               ctx.ImportLog.Log("Skipped: {0}. Date={1}", elt.FullName, elt.LastModified);
               return;
            }
         }

         using (Stream fs = elt.CreateStream())
         {
            ImportStream(ctx, sink, elt, fs);
         }
      }
   }
}
