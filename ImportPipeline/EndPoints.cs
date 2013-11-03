using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

using Bitmanager.Elastic;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Core;

namespace Bitmanager.ImportPipeline
{
   public class EndPoints : NamedAdminCollection<EndPoint>
   {
      private StringDict<IDataEndpoint> endPointCache;
      public EndPoints(ImportEngine engine, XmlNode collNode)
         : base(collNode, "endpoint", (n)=>new ESEndPoint(engine, n), true)
      {
         endPointCache = new StringDict<IDataEndpoint>();
      }

      public IDataEndpoint GetDataEndPoint(String name)
      {
         IDataEndpoint ret; 
         if (endPointCache == null) endPointCache = new StringDict<IDataEndpoint>();
         if (endPointCache.TryGetValue(name, out ret)) return ret;

         int ix = name.IndexOf('.');
         EndPoint endPoint;
         String namePart2 = name;
         if (ix < 0)
            endPoint = this[0];
         else {
            endPoint = this.GetByName (name.Substring(0, ix));
            namePart2 = name.Substring(ix+1);
         }
         ret = endPoint.GetDataEndPoint (namePart2);
         endPointCache.Add (name, ret);
         return ret;
      }
      public void Open(bool isReindex)
      {
         foreach (var x in this) x.Open(isReindex);

         if (endPointCache == null) return;
         foreach (var kvp in endPointCache)
            kvp.Value.Opened();
      }

      public void Close(bool isError)
      {
         foreach (var x in this) x.Close(isError);
      }
   }

   public abstract class EndPoint : NamedItem
   {
      public EndPoint(XmlNode node)
         : base(node)
      {
      }

      public virtual void Open(bool isReindex)
      {
      }
      public virtual void Close(bool isError)
      {
      }
      public abstract IDataEndpoint GetDataEndPoint(string namePart2);
   }

   public interface IDataEndpoint
   {
      void Opened();
      void Clear();
      Object GetField(String fld);
      void SetField(String fld, Object value);
      void Add();
   }

   public class ESDataEndpoint : IDataEndpoint
   {
      public readonly ESEndPoint EndPoint;
      private String urlPart;
      private ESConnection connection;
      Logger addLogger = Logs.CreateLogger("pipelineAdder", "ESDataEndpoint");

      public ESDataEndpoint(ESEndPoint endpoint, String urlPart)
      {
         this.EndPoint = endpoint;
         this.connection = endpoint.Connection;
         this.urlPart = urlPart;
         Clear();
      }

      public void Opened()
      {
         int ix = urlPart.IndexOf('/');
         String index = urlPart.Substring(0, ix);
         String doc = urlPart.Substring(ix + 1);

         var def = EndPoint.Indexes.GetDefinition(index, true);
         urlPart = def.GetPathForUrl(doc, true);
      }

      JObject accumulator;

      public void Clear()
      {
         accumulator = new JObject();
      }

      public Object GetField(String fld)
      {
         addLogger.Log("-- getfield ({0})", fld);
         throw new Exception("notimpl");
      }
      public void SetField(String fld, Object value)
      {
         addLogger.Log("-- setfield {0}: '{1}'", fld, value);
         accumulator.WriteToken(fld, value);
      }

      public void Add()
      {
         addLogger.Log(accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
         connection.Post(urlPart, accumulator).ThrowIfError();
      }

   }

   public class ESEndPoint : EndPoint
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

      public override IDataEndpoint GetDataEndPoint(string namePart2)
      {
         if (namePart2.IndexOf('/') < 0)
            throw new BMException ("Invalid endpoint name '{0}'. Data endpoint name must contain a '/'.", namePart2);
         return new ESDataEndpoint(this, namePart2);
      }
   }
}
