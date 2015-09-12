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
         : base(collNode, "endpoint", (n) => create (engine, n), false)
      {
         this.engine = engine;
         if (GetByName("nop", false) == null)
         {
            Add (new Endpoint("nop")); //always have a NOP endpoint for free
         }
      }

      private static Endpoint create (ImportEngine engine, XmlNode n)
      {
         String type = n.ReadStr("@type");
         if (String.Equals("nop", type, StringComparison.InvariantCultureIgnoreCase)) return new Endpoint(engine, n);
         return ImportEngine.CreateObject<Endpoint>(n, engine, n);
      }



      public IDataEndpoint GetDataEndpoint(PipelineContext ctx, String name, bool mustExcept = true)
      {
         Endpoint ep;
         String dataName = null;
         if (String.IsNullOrEmpty(name))
         {
            ep = base.GetByNamesOrFirst(null, null); //NAMES
            goto CREATE_DATA_ENDPOINT;
         }

         int ix = name.IndexOf('.');
         if (ix < 0)
         {
            ep = this.GetByName(name, mustExcept);
            goto CREATE_DATA_ENDPOINT;
         }

         ep = this.GetByName(name.Substring(0, ix), mustExcept);
         dataName = name.Substring(ix + 1);

      CREATE_DATA_ENDPOINT:
         return ep==null ? null : ep._CheckOpen(ctx)._CreateDataEndpoint(ctx, dataName, mustExcept);
      }

      public bool CheckDataEndpoint(PipelineContext ctx, String name, bool mustExcept = true)
      {
         Endpoint ep;
         String dataName = null;
         if (String.IsNullOrEmpty(name))
         {
            ep = base.GetByNamesOrFirst(null, null); //NAMES
            goto CHECK_DATA_ENDPOINT;
         }

         int ix = name.IndexOf('.');
         if (ix < 0)
         {
            ep = this.GetByName(name, mustExcept);
            goto CHECK_DATA_ENDPOINT;
         }

         ep = this.GetByName(name.Substring(0, ix), mustExcept);
         dataName = name.Substring(ix + 1);

      CHECK_DATA_ENDPOINT:
         return ep == null ? false : ep._CheckDataEndpoint(ctx, dataName, mustExcept);
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
            x._Close(ctx);
      }
      public void CloseFinally(PipelineContext ctx)
      {
         foreach (var x in this)
         {
            try
            {
               x._Close(ctx);
            }
            catch (Exception e)
            {
               ctx.ErrorLog.Log(e, "Close [{0}] failed: ", x.Name);
            }
         }
      }

      /// <summary>
      /// Auto close lazy non-global endpoints on datasource-stop
      /// </summary>
      /// <param name="ctx"></param>
      public void OptClosePerDatasource(PipelineContext ctx)
      {
         foreach (var x in this)
         {
            if (!x.opened) continue;
            if ((x.ActiveMode & ActiveMode.Lazy) == 0) continue;
            if ((x.ActiveMode & ActiveMode.Global) != 0) continue;
            x._Close(ctx);
         }
      }
   }

   [Flags]
   public enum ActiveMode {True=1, False=2, Lazy=4, Global=8, Local=16};
   public enum CloseMode
   {
      Normal = 0,
      NormalCloseOnError = 1,
      NormalCloseOnLimit = 2,
   }

   public class Endpoint : NamedItem
   {
      public enum DebugFlags {_None=0, _LogField=1, _LogAdd=2};
      internal protected bool opened;
      public readonly ActiveMode ActiveMode;
      public readonly DebugFlags Flags;
      public readonly CloseMode CloseMode;
      public readonly ImportEngine Engine;

      internal Endpoint(String name)
         : base(name)
      {
         Flags = DebugFlags._None;
         ActiveMode = ImportPipeline.ActiveMode.Lazy | ImportPipeline.ActiveMode.Global;
         CloseMode = ImportPipeline.CloseMode.Normal;
      }
      public Endpoint(ImportEngine engine, XmlNode node) : this(engine, node, 0) { }
      public Endpoint (ImportEngine engine, XmlNode node, ImportPipeline.ActiveMode defActiveMode=0)
         : base(node)
      {
         Engine = engine;
         if (defActiveMode == 0) defActiveMode = ImportPipeline.ActiveMode.Lazy | ImportPipeline.ActiveMode.Global;
         if ((defActiveMode & (ImportPipeline.ActiveMode.Local | ImportPipeline.ActiveMode.Global)) != 0)
            defActiveMode |= ImportPipeline.ActiveMode.Global;
         Flags = node.ReadEnum<DebugFlags>("@flags", 0);
         CloseMode = node.ReadEnum("@closemode", CloseMode.Normal);
         ActiveMode = node.ReadEnum("@active", defActiveMode);
         switch (ActiveMode)
         {
            case 0:
               ActiveMode = defActiveMode;
               break;
            case ImportPipeline.ActiveMode.False:
            case ImportPipeline.ActiveMode.True:
               break;
            default:
               if ((ActiveMode & (ImportPipeline.ActiveMode.False | ImportPipeline.ActiveMode.True)) != 0)
                  throw new BMNodeException (node, "ActiveMode true/false cannot be combined with other flags. ActiveMode={0}.", ActiveMode);
               if ((ActiveMode & (ImportPipeline.ActiveMode.Local | ImportPipeline.ActiveMode.Global)) != 0)
                  ActiveMode |= defActiveMode & (ImportPipeline.ActiveMode.Local | ImportPipeline.ActiveMode.Global);
               break;
         }
      }

      protected bool isNormalCloseAllowed(PipelineContext ctx)
      {
         if ((ctx.ImportFlags & _ImportFlags.DoNotRename) != 0) return false;
         if (ctx.ErrorState == _ErrorState.OK) return true;
         if ((ctx.ErrorState & _ErrorState.Error) != 0)
            if ((ctx.ImportFlags & _ImportFlags.IgnoreErrors) != 0 || (CloseMode & ImportPipeline.CloseMode.NormalCloseOnError) == 0) return false;
         if ((ctx.ErrorState & _ErrorState.Limited) != 0)
            if ((ctx.ImportFlags & _ImportFlags.IgnoreLimited) != 0 || (CloseMode & ImportPipeline.CloseMode.NormalCloseOnLimit) == 0) return false;
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
      internal IDataEndpoint _CreateDataEndpoint(PipelineContext ctx, string name, bool mustExcept = true)
      {
         return CreateDataEndpoint(ctx, name, mustExcept);
      }
      internal bool _CheckDataEndpoint(PipelineContext ctx, string name, bool mustExcept = true)
      {
         return CheckDataEndpoint(ctx, name, mustExcept);
      }

      protected virtual void Open(PipelineContext ctx)
      {
      }

      protected virtual void Close(PipelineContext ctx)
      {
         opened = false;
      }

      protected virtual IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string name, bool mustExcept = true)
      {
         return new JsonEndpointBase<Endpoint>(this);
      }
      protected virtual bool CheckDataEndpoint(PipelineContext ctx, string name, bool mustExcept = true)
      {
         return true;
      }

      public virtual IAdminEndpoint GetAdminEndpoint(PipelineContext ctx)
      {
         return null;
      }

   }


   public enum FieldFlags
   {
      OverWrite = 1<<0,
      Append = 1<<1,
      ToArray = 1<<2,
      SkipEmpty = 1<<3,
      KeepFirst = 1<<4,
      KeepSmallest = 1<< 5,
      KeepSmallestCaseSensitive = 1 << 6,
      KeepLargest = 1 << 7,
      CaseSensitive = 1<<8,
      Unique = 1<<9,
   }

   public interface IAdminEndpoint
   {
      void SaveAdministration(PipelineContext ctx, List<RunAdministration> admins);
      List<RunAdministration> LoadAdministration(PipelineContext ctx);
   }

   public interface IErrorEndpoint
   {
      void SaveError (PipelineContext ctx, Exception err);
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
      JToken GetFieldAsToken(String fld);
      String GetFieldAsStr(String fld);
      int GetFieldAsInt32(String fld);
      long GetFieldAsInt64(String fld);
      double GetFieldAsDbl(String fld);
      DateTime GetFieldAsDate(String fld);

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
      /// <summary>
      /// Deletes a record from an existing store
      /// </summary>
      void Delete(PipelineContext ctx, String recordKey);
   }

   public interface IEndpointResolver
   {
      IAdminEndpoint GetAdminEndpoint(PipelineContext ctx);
   }

   public class JsonEndpointBase: IDataEndpoint, IEndpointResolver
   {
      protected JObject accumulator;
      protected Endpoint.DebugFlags flags;

      public JsonEndpointBase()
      {
         Clear();
      }

      private Logger _addLogger;
      protected Logger addLogger
      {
         get
         {
            if (_addLogger != null) return _addLogger;
            return _addLogger = Logs.CreateLogger("pipelineAdder", GetType().Name);
         }
      }

      public virtual void Clear()
      {
         accumulator = new JObject();
      }

      public virtual Object GetField(String fld)
      {
         return (String.IsNullOrEmpty(fld)) ? accumulator : accumulator.SelectToken(fld, false).ToNative();
      }
      public virtual JToken GetFieldAsToken(String fld)
      {
         return (String.IsNullOrEmpty(fld)) ? accumulator : accumulator.SelectToken(fld, false);
      }

      public virtual String GetFieldAsStr(String fld)
      {
         if (String.IsNullOrEmpty(fld)) return accumulator.ToString(Newtonsoft.Json.Formatting.Indented);
         JToken token = accumulator.SelectToken(fld);
         return token == null ? null : token.ToString();
      }
      public virtual int GetFieldAsInt32(String fld)
      {
         return accumulator.ReadInt(checkField(fld));
      }
      public virtual long GetFieldAsInt64(String fld)
      {
         return accumulator.ReadInt(checkField(fld));
      }
      public virtual double GetFieldAsDbl(String fld)
      {
         return accumulator.ReadDbl(checkField(fld));
      }
      public virtual DateTime GetFieldAsDate(String fld)
      {
         return accumulator.ReadDate(checkField(fld));
      }
      private static String checkField(String fld)
      {
         if (String.IsNullOrEmpty(fld)) throw new BMException("Empty field is not allowed when asking for a typed field.");
         return fld;
      }

      public virtual void SetField(String fld, Object value, FieldFlags fieldFlags=FieldFlags.OverWrite, String sep=null)
      {
         //Logs.DebugLog.Log("SetField ({0}, {1})", fld, value);
         if (String.IsNullOrEmpty(fld))
         {
            if (value == null) return;
            accumulator = (JObject)value;
            goto EXIT_RTN; 
         }
         if ((flags & Bitmanager.ImportPipeline.Endpoint.DebugFlags._LogField) != 0) addLogger.Log("-- setfield {0}: '{1}'", fld, value);
         //if (value == null) addLogger.Log("Field {0}==null", fld);

         //Test for empty fields
         if ((fieldFlags & FieldFlags.SkipEmpty) != 0)
         {
            if (value == null) goto EXIT_RTN; ;
            String tmp = value as String;
            if (tmp != null && tmp.Length == 0) goto EXIT_RTN; ;
         }

         switch (fieldFlags & (FieldFlags.Append | FieldFlags.OverWrite | FieldFlags.ToArray | FieldFlags.KeepFirst | FieldFlags.KeepSmallest | FieldFlags.KeepLargest))
         {
            case 0:
            case FieldFlags.OverWrite:
               goto WRITE_TOKEN;

            case FieldFlags.KeepFirst:
               Object obj = accumulator[fld];
               if (obj != null) goto EXIT_RTN;
               goto WRITE_TOKEN;

            case FieldFlags.KeepSmallest:
               if (compareToken (accumulator[fld], value, fieldFlags) <= 0) goto EXIT_RTN;
               goto WRITE_TOKEN;

            case FieldFlags.KeepLargest:
               if (compareToken (accumulator[fld], value, fieldFlags) >= 0) goto EXIT_RTN;
               goto WRITE_TOKEN;

            case FieldFlags.Append:
               String existing = accumulator.ReadStr (fld, null);
               if (existing == null) goto WRITE_TOKEN;
               accumulator.WriteToken(fld, existing + sep + value);
               goto EXIT_RTN;

            default:
               JToken token = accumulator.SelectToken(fld, false);
               JArray arr = token as JArray;
               if (arr != null)
               {
                  if (0 != (fieldFlags & FieldFlags.Unique))
                      addUnique(arr, value, fieldFlags); 
                  else
                     arr.Add(value); 
                  goto EXIT_RTN;
               }
               arr = accumulator.AddArray (fld);
               if (token != null) arr.Add(token);
               arr.Add(value);
               return;
         }

         WRITE_TOKEN:
         accumulator.WriteToken(fld, value);
         EXIT_RTN:;
      }

      protected static StringComparison toComparison(FieldFlags flags)
      {
         return (flags & FieldFlags.KeepSmallestCaseSensitive) != 0 ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
      }
      protected static void addUnique(JArray arr, Object value, FieldFlags flags)
      {
         StringComparison comparison = toComparison(flags);
         String v = value == null ? null : value.ToString();
         for (int i = 0; i < arr.Count; i++)
         {
            if (String.Equals((String)arr[i], v, comparison)) return;
         }
         arr.Add(value);
         return;
      }

      protected static int compareToken(JToken token, Object value, FieldFlags flags)
      {
         if (value == null)
         {
            return token == null ? 0 : -1;
         }
         if (token == null) return 1;

         //For strings: just compare the length
         var t1 = token.Type;
         if (value is String || t1==JTokenType.String)
            return value.ToString().Length - token.ToString().Length;

         JToken valToken = JToken.FromObject(value);
         var t2 = valToken.Type;
         if (t1 == t2)
         {
            switch (t1)
            {
               case JTokenType.Boolean: return Comparer<bool>.Default.Compare((bool)token, (bool)valToken);
               case JTokenType.Date: return Comparer<DateTime>.Default.Compare((DateTime)token, (DateTime)valToken);
               case JTokenType.Float: return Comparer<double>.Default.Compare((double)token, (double)valToken);
               case JTokenType.Integer: return Comparer<long>.Default.Compare((long)token, (long)valToken);
            }
         }
         return String.Compare(token.ToString(), value.ToString(), toComparison(flags));
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
      public virtual void Delete(PipelineContext ctx, String recordKey)
      {
         throw new BMException ("Deletes are not supported by endpoint [{0}]. Delete key={1}.", GetType().FullName, recordKey);
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

      #region IEndpointResolver
      public virtual IAdminEndpoint GetAdminEndpoint(PipelineContext ctx)
      {
         return null;
      }
      #endregion

   }



   public class JsonEndpointBase<T> : JsonEndpointBase, IEndpointResolver where T : Endpoint
   {
      public readonly T Endpoint;

      public JsonEndpointBase(T endpoint)
      {
         Endpoint = endpoint;
         flags = endpoint == null ? 0 : endpoint.Flags;
      }

      #region IEndpointResolver
      public override IAdminEndpoint GetAdminEndpoint(PipelineContext ctx)
      {
         return Endpoint==null ? null : Endpoint.GetAdminEndpoint(ctx);
      }
      #endregion
   }

}
