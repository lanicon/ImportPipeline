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
   public class Endpoints : NamedAdminCollection<Endpoint>
   {
      public readonly ImportEngine engine;
      public Endpoints(ImportEngine engine, XmlNode collNode)
         : base(collNode, "endpoint", (n) => ImportEngine.CreateObject<Endpoint>(n, engine, n), true)
      {
         this.engine = engine;
      }

      public IDataEndpoint GetDataEndpoint(PipelineContext ctx, String name)
      {
         Endpoint ep;
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
         return ep._CheckOpen(ctx)._CreateDataEndpoint(ctx, dataName); 
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

      public void Close(PipelineContext ctx)
      {
         foreach (var x in this) 
            x._Close (ctx);
      }
   }

   public enum ActiveMode {True, False, Lazy};
   public enum CloseMode
   {
      Normal = 0,
      NormalCloseOnError = 1,
      NormalCloseOnLimit = 2,
   }

   public class Endpoint : NamedItem
   {
      public enum DebugFlags {_None=0, _LogField=1, _LogAdd=2};
      internal bool opened;
      public readonly ActiveMode ActiveMode;
      public readonly DebugFlags Flags;
      public readonly CloseMode CloseMode;

      public Endpoint(ImportEngine engine, XmlNode node) : this(node) { }
      public Endpoint(XmlNode node)
         : base(node)
      {
         Flags = node.OptReadEnum<DebugFlags>("@flags", 0);
         ActiveMode = node.OptReadEnum("@active", ActiveMode.Lazy);
         CloseMode = node.OptReadEnum("@closemode", CloseMode.Normal);
      }

      protected bool isNormalCloseAllowed(PipelineContext ctx)
      {
         if ((ctx.ImportFlags & _ImportFlags.DoNotRename) != 0) return false;
         if (ctx.ErrorState == _ErrorState.OK) return true;
         if ((ctx.ErrorState & _ErrorState.Error) != 0 && (CloseMode & ImportPipeline.CloseMode.NormalCloseOnError) == 0) return false;
         if ((ctx.ErrorState & _ErrorState.Limited) != 0 && (CloseMode & ImportPipeline.CloseMode.NormalCloseOnLimit) == 0) return false;
         return true;
      }

      protected bool logCloseAndCheckForNormalClose(PipelineContext ctx)
      {
         ctx.ImportLog.Log("Closing endpoint '{0}', error={1}, flags={2}, closemode={3}", Name, ctx.ErrorState, ctx.ImportFlags, CloseMode);
         if (!isNormalCloseAllowed(ctx))
         {
            ctx.ImportLog.Log("-- Normal closing prevented by flags or state");
            return false;
         }
         return true;
      }

      protected void logCloseDone(PipelineContext ctx)
      {
         ctx.ImportLog.Log("-- Closed");
      }



      internal Endpoint _OptionalOpen(PipelineContext ctx)
      {
         return (ActiveMode == ImportPipeline.ActiveMode.True) ? _Open (ctx): this;
      }
      internal Endpoint _CheckOpen(PipelineContext ctx)
      {
         if (opened) return this;
         if (ActiveMode == ImportPipeline.ActiveMode.False)
            throw new BMException("Cannot open endpoint '{0}' because it is not active.", Name);
         return _Open(ctx);
      }

      private Endpoint _Open(PipelineContext ctx)
      {
         ctx.ImportEngine.ImportLog.Log ("Opening endpoint '{0}'...", Name);
         Open(ctx);
         opened = true;
         return this;
      }
      internal Endpoint _Close(PipelineContext ctx)
      {
         if (!opened) return this;
         ctx.ImportEngine.ImportLog.Log("Closing endpoint '{0}'...", Name);
         Close(ctx);
         return this;
      }
      internal IDataEndpoint _CreateDataEndpoint(PipelineContext ctx, string name)
      {
         return CreateDataEndpoint(ctx, name);
      }

      protected virtual void Open(PipelineContext ctx)
      {
      }

      protected virtual void Close(PipelineContext ctx)
      {
      }

      protected virtual IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string name)
      {
         return new JsonEndpointBase<Endpoint>(this);
      }
   }


   public enum FieldFlags
   {
      OverWrite = 1<<0,
      Append = 1<<1,
      ToArray = 1<<2,
      SkipEmpty = 1<<3,
   }

   /// <summary>
   /// Interface that is called from the pipeline to save fields and add records
   /// </summary>
   public interface IDataEndpoint
   {
      /// <summary>
      /// Start notification (called when the pipeline is started)
      /// </summary>
      void Start(PipelineContext ctx);
      /// <summary>
      /// Stop notification (called when the pipeline is started)
      /// </summary>
      void Stop(PipelineContext ctx);
      /// <summary>
      /// Clears all saved content
      /// </summary>
      void Clear();
      /// <summary>
      /// Gets the content of a field. If fld==null, the whole cached content is returned (if supported)
      /// </summary>
      Object GetField(String fld);

      /// <summary>
      /// Sets the content of a field. If fld==null, the whole cached content is replaced (if supported)
      /// </summary>
      void SetField(String fld, Object value, FieldFlags flags=FieldFlags.OverWrite, String sep=null);
      /// <summary>
      /// Adds the content as a record
      /// </summary>
      void Add(PipelineContext ctx);
      /// <summary>
      /// Checks whether a record exists or not, or with a different date
      /// </summary>
      ExistState Exists(PipelineContext ctx, String key, DateTime? timeStamp);
      /// <summary>
      /// Loads a record by the supplied key
      /// </summary>
      Object LoadRecord(PipelineContext ctx, String key);
      /// <summary>
      /// Breaks a record into pieces and emits the pieces to the pipeline
      /// </summary>
      void EmitRecord(PipelineContext ctx, String recordKey, String recordField, IDatasourceSink sink, String eventKey, int maxLevel);

   }

   public class JsonEndpointBase<T> : IDataEndpoint where T: Endpoint
   {
      public readonly T Endpoint;
      protected Logger addLogger;
      protected JObject accumulator;
      protected Endpoint.DebugFlags flags;

      public JsonEndpointBase(T endpoint)
      {
         Endpoint = endpoint;
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
         return (String.IsNullOrEmpty(fld)) ? accumulator : accumulator.SelectToken (fld, false);
      }

      public virtual void SetField(String fld, Object value, FieldFlags fieldFlags, String sep)
      {
         if (String.IsNullOrEmpty(fld))
         {
            if (value == null) return;
            accumulator = (JObject)value;
         }
         if ((flags & Bitmanager.ImportPipeline.Endpoint.DebugFlags._LogField) != 0) addLogger.Log("-- setfield {0}: '{1}'", fld, value);
         //if (value == null) addLogger.Log("Field {0}==null", fld);

         //Test for empty fields
         if ((fieldFlags & FieldFlags.SkipEmpty) != 0)
         {
            if (value==null) return;
            String tmp = value as String;
            if (tmp != null && tmp.Length==0) return;
         }

         switch (fieldFlags & (FieldFlags.Append | FieldFlags.OverWrite | FieldFlags.ToArray))
         {
            case 0:
            case FieldFlags.OverWrite:
               accumulator.WriteToken(fld, value);
               return;

            case FieldFlags.Append:
               String existing = accumulator.ReadStr (fld, null);
               if (existing == null)
               {
                  accumulator.WriteToken(fld, value);
                  return;
               }
               accumulator.WriteToken(fld, existing + sep + value);
               return;

            default:
               JToken token = accumulator.SelectToken(fld, false);
               JArray arr = token as JArray;
               if (arr != null)
               {
                  arr.Add (value);
                  return;
               }
               arr = accumulator.AddArray (fld);
               if (token != null) arr.Add(token);
               arr.Add(value);
               return;
         }
      }
      

      protected void OptLogAdd()
      {
         if ((flags & Bitmanager.ImportPipeline.Endpoint.DebugFlags._LogAdd) != 0)
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
