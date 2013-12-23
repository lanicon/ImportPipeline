using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bitmanager.Elastic;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Core;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public class ESEndPoint: EndPoint
   {
      public readonly ESConnection Connection;
      public readonly IndexDefinitionTypes IndexTypes;
      public readonly IndexDefinitions Indexes;

      private ESIndexCmd._CheckIndexFlags flags;

      public ESEndPoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         Connection = new ESConnection(node.ReadStr("@url"));
         IndexTypes = new IndexDefinitionTypes(engine.Xml, node.SelectMandatoryNode("indextypes"));
         Indexes = new IndexDefinitions(IndexTypes, node.SelectMandatoryNode("indexes"));
      }

      public override void Open(bool isReindex)
      {
         if (isReindex) flags = ESIndexCmd._CheckIndexFlags.ForceCreate | ESIndexCmd._CheckIndexFlags.AppendDate;
         Indexes.CreateIndexes(Connection, flags);
         WaitForGreenOrYellow();
      }
      public override void Close(bool isError)
      {
         if (isError) return;
         Indexes.OptionalRename(Connection);
      }

      private const int TIMEOUT = 30000;
      public bool WaitForGreenOrYellow(bool mustExcept = true)
      {
         return Connection.CreateHealthRequest().WaitForStatus(ClusterStatus.Green, ClusterStatus.Yellow, TIMEOUT, mustExcept);
      }
      public bool WaitForGreen(bool mustExcept = true)
      {
         return Connection.CreateHealthRequest().WaitForStatus(ClusterStatus.Green, TIMEOUT, mustExcept);

      }

      public override IDataEndpoint CreateDataEndPoint(string namePart2)
      {
         if (namePart2.IndexOf('/') < 0)
            throw new BMException ("Invalid endpoint name '{0}'. Data endpoint name must contain a '/'.", namePart2);
         return new ESDataEndpoint(this, namePart2);
      }
   }


   public class ESDataEndpoint : JsonEndpointBase<ESEndPoint>
   {
      private String urlPart;
      private ESConnection connection;

      public ESDataEndpoint(ESEndPoint endpoint, String urlPart): base (endpoint)
      {
         this.connection = endpoint.Connection;
         this.urlPart = urlPart;
      }

      public override void Opened()
      {
         int ix = urlPart.IndexOf('/');
         String index = urlPart.Substring(0, ix);
         String doc = urlPart.Substring(ix + 1);

         var def = EndPoint.Indexes.GetDefinition(index, true);
         urlPart = def.GetPathForUrl(doc, true);
      }

      public override void Add()
      {
         addLogger.Log(accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
         connection.Post(urlPart, accumulator).ThrowIfError();
      }

   }

}
