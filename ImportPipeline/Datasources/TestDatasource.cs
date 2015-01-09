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
using System.Net;

namespace Bitmanager.ImportPipeline
{
   public class TestDatasource : Datasource
   {
      public void Init(PipelineContext ctx, XmlNode node)
      {
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         sink.HandleValue(ctx, "record/double", 123.45);
         sink.HandleValue(ctx, "record/date", DateTime.Now);
         sink.HandleValue(ctx, "record/utcdate", DateTime.UtcNow);
         sink.HandleValue(ctx, "record/int", -123);
         sink.HandleValue(ctx, "record/string", "foo bar");
         sink.HandleValue(ctx, "record", null);
      }
   }
}
