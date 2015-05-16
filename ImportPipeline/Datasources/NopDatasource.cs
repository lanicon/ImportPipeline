using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Data.SqlClient;
using System.Data;
using MySql.Data.MySqlClient;
using System.Data.Common;
using System.Data.Odbc;

namespace Bitmanager.ImportPipeline
{
   public class NopDatasource : Datasource
   {
      public void Init(PipelineContext ctx, XmlNode node)
      {
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
      }
   }
}
