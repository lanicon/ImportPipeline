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
   public class CdoDatasource : StreamDatasourceBase
   {
      protected override void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm)
      {
         CDO.IMessage msg = new CDO.Message();
         msg.DataSource.OpenObject(new IStreamFromStream(strm), "IStream");
         sink.HandleValue(ctx, "record/subject", msg.Subject);
         sink.HandleValue(ctx, "record/bcc", msg.BCC);
         sink.HandleValue(ctx, "record/cc", msg.CC);
         sink.HandleValue(ctx, "record/from", msg.From);
         sink.HandleValue(ctx, "record/to", msg.To);
         Utils.FreeAndNil(ref msg);
         sink.HandleValue(ctx, "record", null);
         ctx.IncrementEmitted();
      }

   }
}
