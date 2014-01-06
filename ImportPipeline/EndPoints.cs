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
         if (name == null) return null;
         IDataEndpoint ret;
         if (endPointCache == null) endPointCache = new StringDict<IDataEndpoint>();
         if (endPointCache.TryGetValue(name, out ret)) return ret;

         EndPoint endPoint = this[0];

         if (String.IsNullOrEmpty(name))
            ret = endPoint.CreateDataEndPoint(null);
         else
         {
            int ix = name.IndexOf('.');
            if (ix < 0) 
            {
               endPoint = this.GetByName(name);
               ret = endPoint.CreateDataEndPoint(null);
            }
            else
            {
               endPoint = this.GetByName(name.Substring(0, ix));
               ret = endPoint.CreateDataEndPoint(name.Substring(ix + 1));
            }
         }
         endPoint.touched = true;
         endPointCache.Add(name, ret); //PW cache at this point is a MT issue!!!
         return ret;
      }

      public void Open(PipelineContext ctx)
      {
         String fmt = "{0}\r\nEndpoint={1}.";
         String name = null;
         engine.ImportLog.Log("Opening endpoints");
         try
         {
            foreach (var x in this)
            {
               name = x.Name;
               engine.ImportLog.Log("-- endpoint '{0}' mustopen={1}.", x.Name, x.touched);
               if (x.touched) x.Open(ctx);
            }
            if (endPointCache == null) return;

            fmt = "{0}\r\nData-Endpoint={1}.";
            engine.ImportLog.Log("Opening datapoints");
            foreach (var kvp in endPointCache)
            {
               name = kvp.Key;
               engine.ImportLog.Log("-- datapoint '{0}'.", kvp.Key);
               kvp.Value.Opened(ctx);
            }
         }
         catch (Exception err)
         {
            throw new BMException(err, fmt, err.Message, name);
         }
      }

      public void Close(PipelineContext ctx, bool isError)
      {
         foreach (var x in this) 
            if (x.touched) x.Close(ctx, isError);
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

      public virtual void Open(PipelineContext ctx)
      {
      }
      public virtual void Close(PipelineContext ctx, bool isError)
      {
      }
      public virtual IDataEndpoint CreateDataEndPoint(string namePart2)
      {
         return new JsonEndpointBase<EndPoint>(this);
      }
   }

   public interface IDataEndpoint
   {
      void Opened(PipelineContext ctx);
      void Clear();
      Object GetField(String fld);
      void SetField(String fld, Object value);
      void Add(PipelineContext ctx);
      ExistState Exists(PipelineContext ctx, String key, DateTime? timeStamp);
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
         if (value == null) addLogger.Log("Field {0}==null", fld);
           accumulator.WriteToken(fld, value);
      }

      protected void OptLogAdd()
      {
         if ((flags & Bitmanager.ImportPipeline.EndPoint.DebugFlags._LogAdd) != 0)
            addLogger.Log("Add: " + accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
      }
      public virtual void Add(PipelineContext ctx)
      {
         OptLogAdd();
         Clear();
      }

      public virtual void Opened(PipelineContext ctx)
      {
      }


      public virtual ExistState Exists(PipelineContext ctx, string key, DateTime? timeStamp)
      {
         return ExistState.NotExist;
      }
   }
}
