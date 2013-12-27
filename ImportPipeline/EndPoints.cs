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
      public readonly ImportEngine engine;
      public EndPoints(ImportEngine engine, XmlNode collNode)
         : base(collNode, "endpoint", (n) => ImportEngine.CreateObject<EndPoint>(n, engine, n), true)
      {
         endPointCache = new StringDict<IDataEndpoint>();
         this.engine = engine;
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
         else
         {
            endPoint = this.GetByName(name.Substring(0, ix));
            namePart2 = name.Substring(ix + 1);
         }
         endPoint.touched = true;
         ret = endPoint.CreateDataEndPoint(namePart2);
         endPointCache.Add(name, ret);
         return ret;
      }

      public void Open(bool isReindex)
      {
         engine.ImportLog.Log("Opening endpoints");
         foreach (var x in this)
         {
            engine.ImportLog.Log("-- endpoint '{0}' mustopen={1}.", x.Name, x.touched);
            if (x.touched) x.Open(isReindex);
         }
         if (endPointCache == null) return;

         engine.ImportLog.Log("Opening datapoints");
         foreach (var kvp in endPointCache)
         {
            engine.ImportLog.Log("-- datapoint '{0}'.", kvp.Key);
            kvp.Value.Opened();
         }
      }

      public void Close(bool isError)
      {
         foreach (var x in this) 
            if (x.touched) x.Close(isError);
      }
   }

   public enum ActiveMode {Active, Lazy, Inactive};
   public class EndPoint : NamedItem
   {
      public enum DebugFlags {_None=0, _LogField=1, _LogAdd=2};
      internal bool touched;
      public readonly bool Active;
      public readonly DebugFlags Flags;

      public EndPoint(ImportEngine engine, XmlNode node) : this(node) { }
      public EndPoint(XmlNode node)
         : base(node)
      {
         Flags = node.OptReadEnum<DebugFlags>("@flags", 0);
         String act = node.OptReadStr("@active", "lazy").ToLowerInvariant();
         switch (act)
         {
            case "lazy":
               Active = true;
               break;
            case "true":
               Active = true;
               touched = true;
               break;
            case "false":
               break;
            default:
               throw new BMNodeException(node, "Invalid value for @active: {0}. Possible values: true, false, lazy.", act);
         }
      }

      public virtual void Open(bool isReindex)
      {
      }
      public virtual void Close(bool isError)
      {
      }
      public virtual IDataEndpoint CreateDataEndPoint(string namePart2)
      {
         return new JsonEndpointBase<EndPoint>(this);
      }
   }

   public interface IDataEndpoint
   {
      void Opened();
      void Clear();
      Object GetField(String fld);
      void SetField(String fld, Object value);
      void Add();
   }

   public class JsonEndpointBase<T> : IDataEndpoint where T: EndPoint
   {
      public readonly T EndPoint;
      protected Logger addLogger;
      protected JObject accumulator;
      protected EndPoint.DebugFlags flags;

      public JsonEndpointBase(T endpoint)
      {
         EndPoint = endpoint;
         flags = endpoint.Flags;
         addLogger = Logs.CreateLogger("pipelineAdder", GetType().Name);
         Clear();
      }

      public virtual void Clear()
      {
         accumulator = new JObject();
      }

      public virtual Object GetField(String fld)
      {
         throw new Exception("notimpl");
      }

      public virtual void SetField(String fld, Object value)
      {
         if ((flags & Bitmanager.ImportPipeline.EndPoint.DebugFlags._LogField) != 0) addLogger.Log("-- setfield {0}: '{1}'", fld, value);
         accumulator.WriteToken(fld, value);
      }

      public virtual void Add()
      {
         if ((flags & Bitmanager.ImportPipeline.EndPoint.DebugFlags._LogAdd) == 0) return;
         addLogger.Log("Add: " + accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
         Clear();
      }

      public virtual void Opened()
      {
      }
   }
}
