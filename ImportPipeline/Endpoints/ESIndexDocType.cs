using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Core;
using Bitmanager.Xml;
using Bitmanager.Json;
using System.Text.RegularExpressions;
using System.Web;
using System.Net;
using Bitmanager.Elastic;

namespace Bitmanager.ImportPipeline
{
   public class ESIndexDocType
   {
      public readonly String Name;
      public readonly String TypeName;
      public readonly ESIndexDefinition Index;
      public readonly String KeyFieldName;
      public readonly String DateFieldName;
      //private IndexDefinition indexDefinition;
      //private XmlNode xmlNode;
      public String UrlPart { get { return Index.IndexName + "/" + TypeName; } }
      public String UrlPartForPreviousIndex { get { return Index.AliasName + "/" + TypeName; } }

      private ESIndexDocType(XmlNode node)
      {
         Name = node.ReadStr("@name");
         TypeName = node.ReadStr("@typename", Name);
         KeyFieldName = node.ReadStr("@keyfield", null);
         DateFieldName = node.ReadStr("@datefield", null);
      }

      public ESIndexDocType(ESIndexDefinitions indexes, XmlNode node)
         : this(node)
      {
         String indexName = node.ReadStr("@index");
         Index = indexes.GetDefinition(indexName, false);
         if (Index == null) throw new BMNodeException(node, "Index '{0}' not found.", indexName);
      }

      internal ESIndexDocType(ESIndexDefinition indexDefinition, XmlNode node)
         : this(node)
      {
         Index = indexDefinition;
      }

      public ExistState Exists(ESConnection conn, String key, DateTime? timeStamp = null)
      {
         JObject record = null;
         ESMainResponse resp;
         bool dateNeeded = DateFieldName != null && timeStamp != null;

         if (KeyFieldName == null) goto NOT_EXIST; ;
         if (KeyFieldName.Equals("_id", StringComparison.InvariantCultureIgnoreCase))
         {
            String url = UrlPart + "/" + HttpUtility.UrlEncode(key);
            if (dateNeeded) url += "?fields=" + HttpUtility.UrlEncode(DateFieldName);
            resp = conn.Get(url);
            if (resp.StatusCode == HttpStatusCode.NotFound) goto NOT_EXIST;
            resp.ThrowIfError();
            if (!resp.JObject.ReadBool("found")) goto NOT_EXIST;
            if (!dateNeeded) return ExistState.Exist;
            record = resp.JObject;
         }
         else
         {
            JObject req = createMatchQueryRequest(KeyFieldName, key);
            req.AddArray("_source").Add(DateFieldName);
            resp = conn.Post(UrlPart + "/_search", req);
            if (resp.StatusCode == System.Net.HttpStatusCode.NotFound) goto NOT_EXIST;
            resp.ThrowIfError();
            JArray hits = (JArray)resp.JObject.SelectToken("hits.hits", false);
            if (hits == null || hits.Count == 0) goto NOT_EXIST;

            if (!dateNeeded) return ExistState.Exist;
            record = (JObject)hits[0];
         }
         DateTime dt = record.ReadDate("fields." + DateFieldName, DateTime.MinValue);
         if (dt == DateTime.MinValue)
         {
            if (conn.Logger != null) conn.Logger.Log(_LogType.ltWarning, "Exists: Record without field [" + DateFieldName + "] returned.");
            return ExistState.NotExist;
         }
         //Logs.DebugLog.Log("Record=" + record);
         //Logs.DebugLog.Log("RecDate={0}, Timestamp={1}, cmp={2}", date, timeStamp, Comparer<DateTime>.Default.Compare((DateTime)date, (DateTime)timeStamp));
         if (dt < timeStamp) return ExistState.ExistOlder;
         if (dt > timeStamp) return ExistState.ExistNewer;
         return ExistState.ExistSame;

      NOT_EXIST:
         return ExistState.NotExist;
      }

      public JObject LoadByKey(ESConnection conn, String key)
      {
         JObject record = null;
         ESMainResponse resp;
         if (KeyFieldName == null) goto NOT_EXIST; ;
         if (KeyFieldName.Equals("_id", StringComparison.InvariantCultureIgnoreCase))
         {
            String url = UrlPart + "/" + HttpUtility.UrlEncode(key);
            resp = conn.Get(url);
            if (resp.StatusCode == HttpStatusCode.NotFound) goto NOT_EXIST;
            resp.ThrowIfError();
            if (!resp.JObject.ReadBool("found")) goto NOT_EXIST;
            record = resp.JObject;
         }
         else
         {
            var req = createMatchQueryRequest(KeyFieldName, key);
            resp = conn.Post(UrlPart + "/_search", req);
            if (resp.StatusCode == System.Net.HttpStatusCode.NotFound) goto NOT_EXIST;
            resp.ThrowIfError();
            JArray hits = (JArray)resp.JObject.SelectToken("hits.hits", false);
            if (hits == null || hits.Count == 0) goto NOT_EXIST;

            record = (JObject)hits[0];
         }
         Logs.DebugLog.Log("Key={0}: Record={1}", key, record);
         return (JObject)record.SelectToken("_source", false);

      NOT_EXIST:
         return null;
      }

      private JObject createMatchQueryRequest(String field, String value)
      {
         var m = new JObject();
         m[field] = value;
         var q = new JObject();
         q["match"] = m;
         var ret = new JObject();
         ret["query"] = q;
         return ret;
      }
   }

   public class ESIndexDocTypes : IEnumerable<ESIndexDocType>
   {
      private List<ESIndexDocType> list;
      private StringDict<ESIndexDocType> dict;

      public ESIndexDocTypes()
      {
         list = new List<ESIndexDocType>();
         dict = new StringDict<ESIndexDocType>();
      }
      public ESIndexDocTypes(ESIndexDefinitions indexDefs, XmlNode node)
      {
         XmlNodeList nodes = node.SelectMandatoryNodes("type");
         list = new List<ESIndexDocType>(nodes.Count);
         dict = new StringDict<ESIndexDocType>(nodes.Count);
         Load(indexDefs, nodes);
      }

      public void Load(ESIndexDefinitions indexDefs, XmlNode node)
      {
         Load(indexDefs, node.SelectNodes("type"));
      }
      public void Load(ESIndexDefinitions indexDefs, XmlNodeList nodes)
      {
         foreach (XmlNode x in nodes) Add(new ESIndexDocType(indexDefs, x));
      }

      public ESIndexDocType Add(ESIndexDocType x)
      {
         dict.Add(x.Name, x);
         list.Add(x);
         return x;
      }

      public int Count { get { return list.Count; } }
      public ESIndexDocType this[int index] { get { return list[index]; } }

      public ESIndexDocType GetDocType(String name, bool mustExcept = true)
      {
         ESIndexDocType x;
         if (name != null && dict.TryGetValue(name, out x)) return x;

         if (!mustExcept) return null;
         throw new BMException("DocType '{0}' is not defined in the config.", name);
      }

      public IEnumerator<ESIndexDocType> GetEnumerator()
      {
         return list.GetEnumerator();
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return list.GetEnumerator();
      }

   }


}
