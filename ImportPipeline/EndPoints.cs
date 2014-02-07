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
      public readonly ImportEngine engine;
      public EndPoints(ImportEngine engine, XmlNode collNode)
         : base(collNode, "endpoint", (n) => ImportEngine.CreateObject<EndPoint>(n, engine, n), true)
      {
         this.engine = engine;
      }

      public IDataEndpoint GetDataEndPoint(PipelineContext ctx, String name)
      {
         EndPoint ep;
         String dataName = null;
         if (String.IsNullOrEmpty(name))
         {
            ep = base.GetByNamesOrFirst (null, null);
            goto CREATE_DATA_ENDPOINT;
         }

         int ix = name.IndexOf('.');
         if (ix < 0) 
         {
            ep = this.GetByName(name);
            goto CREATE_DATA_ENDPOINT;
         }

         ep = this.GetByName(name.Substring(0, ix));
         dataName = name.Substring(ix + 1);

      CREATE_DATA_ENDPOINT:
         return ep._CheckOpen(ctx)._CreateDataEndPoint(ctx, dataName); 
      }

      public void Open(PipelineContext ctx)
      {
         String name = null;
         engine.ImportLog.Log("Opening active, non-lazy endpoints");
         try
         {
            foreach (var x in this)
            {
               name = x.Name;
               x._OptionalOpen(ctx);
            }
         }
         catch (Exception err)
         {
            throw new BMException(err, "{0}\r\nEndpoint={1}.", err.Message, name);
         }
      }

      public void Close(PipelineContext ctx, bool isError)
      {
         foreach (var x in this) 
            x._Close (ctx, isError);
      }
   }

   public enum ActiveMode {True, False, Lazy};
   public class EndPoint : NamedItem
   {
      public enum DebugFlags {_None=0, _LogField=1, _LogAdd=2};
      internal bool opened;
      public readonly ActiveMode ActiveMode;
      public readonly DebugFlags Flags;

      public EndPoint(ImportEngine engine, XmlNode node) : this(node) { }
      public EndPoint(XmlNode node)
         : base(node)
      {
         Flags = node.OptReadEnum<DebugFlags>("@flags", 0);
         ActiveMode = node.OptReadEnum("@active", ActiveMode.Lazy);
      }

      internal EndPoint _OptionalOpen(PipelineContext ctx)
      {
         return (ActiveMode == ImportPipeline.ActiveMode.True) ? _Open (ctx): this;
      }
      internal EndPoint _CheckOpen(PipelineContext ctx)
      {
         if (opened) return this;
         if (ActiveMode == ImportPipeline.ActiveMode.False)
            throw new BMException("Cannot open endpoint '{0}' because it is not active.", Name);
         return _Open(ctx);
      }

      private EndPoint _Open(PipelineContext ctx)
      {
         ctx.ImportEngine.ImportLog.Log ("Opening endpoint '{0}'...", Name);
         Open(ctx);
         opened = true;
         return this;
      }
      internal EndPoint _Close(PipelineContext ctx, bool isError)
      {
         if (!opened) return this;
         ctx.ImportEngine.ImportLog.Log("Closing endpoint '{0}'...", Name);
         Close(ctx, isError);
         return this;
      }
      internal IDataEndpoint _CreateDataEndPoint(PipelineContext ctx, string name)
      {
         return CreateDataEndPoint(ctx, name);
      }

      protected virtual void Open(PipelineContext ctx)
      {
      }

      protected virtual void Close(PipelineContext ctx, bool isError)
      {
      }

      protected virtual IDataEndpoint CreateDataEndPoint(PipelineContext ctx, string name)
      {
         return new JsonEndpointBase<EndPoint>(this);
      }
   }

   public interface IDataEndpoint
   {
      void Start(PipelineContext ctx);
      void Stop(PipelineContext ctx);
      void Clear();
      Object GetField(String fld);
      void SetField(String fld, Object value);
      void Add(PipelineContext ctx);
      ExistState Exists(PipelineContext ctx, String key, DateTime? timeStamp);
      Object LoadRecord(PipelineContext ctx, String key);
      void EmitRecord(PipelineContext ctx, String recordKey, String recordField, IDatasourceSink sink, String eventKey, int maxLevel);

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
         //if (value == null) addLogger.Log("Field {0}==null", fld);
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

      public virtual void Start(PipelineContext ctx)
      {
      }
      public virtual void Stop(PipelineContext ctx)
      {
      }


      public virtual ExistState Exists(PipelineContext ctx, string key, DateTime? timeStamp)
      {
         return ExistState.NotExist;
      }
      public virtual Object LoadRecord(PipelineContext ctx, String key)
      {
         return null;
      }
      public virtual void EmitRecord(PipelineContext ctx, String recordKey, String recordField, IDatasourceSink sink, String eventKey, int maxLevel)
      {
      }


   }
}
