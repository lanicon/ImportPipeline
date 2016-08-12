/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
      public readonly String AutoTimestampFieldName;
      public readonly String IdPath;
      public readonly String RoutingPath;
      public bool IndexExists { get { return Index.IndexExists; } }
      public String UrlPart { get { return IndexExists ? Index.IndexName + "/" + TypeName : "__not_existing__"; } }
      public String UrlPartForPreviousIndex { get { return IndexExists ? Index.AliasName + "/" + TypeName : "__not_existing__"; } }

      public ESIndexDocType(ESIndexDefinition indexDefinition, XmlNode node)
      {
         Index = indexDefinition;

         Name = node.ReadStr("@name");
         TypeName = node.ReadStr("@typename", Name);
         KeyFieldName = node.ReadStr("@keyfield", null);
         DateFieldName = node.ReadStr("@datefield", null);
         IdPath = node.ReadStr("@idfield", null);
         if (IdPath == null) IdPath = node.ReadStr("@idpath", null);
         RoutingPath = node.ReadStr("@routingfield", null);
         AutoTimestampFieldName = node.ReadStr("@ts", indexDefinition.AutoTimestampFieldName);
      }

      public JObject GetCmdForBulk(JObject obj, String verb="index")
      {
         String id = IdPath == null ? null : (String)obj[IdPath];
         String routing = RoutingPath == null ? null : (String)obj[RoutingPath];
         if (id == null && routing == null) return null;

         JObject ret = new JObject();
         JObject x = new JObject();
         if (id != null) x["_id"] = id;
         if (routing != null) x["_routing"] = routing;
         ret.Add(verb, x);
         return ret;
      }

      public String GetUrlForAdd(JObject obj)
      {
         if (IdPath == null) return UrlPart;
         String id = (String)obj[IdPath];
         return id == null ? UrlPart : UrlPart + "/" + HttpUtility.UrlEncode(id);
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
         DateTime dt = record.ReadDate("_source." + DateFieldName, DateTime.MinValue);
         if (dt == DateTime.MinValue)
         {
            if (conn.Logger != null) conn.Logger.Log(_LogType.ltWarning, "Exists: Record without field [" + DateFieldName + "] returned.");
            return ExistState.Exist;
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
         if (!IndexExists) goto NOT_EXIST;

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

      private bool deleteById (ESConnection conn, String key)
      {
         if (!IndexExists) return false;

         String url = UrlPart + "/" + HttpUtility.UrlEncode(key);
         ESMainResponse resp = conn.Delete(url);
         Logs.DebugLog.Log("DeleteById ({0}) stat={1}", key, resp.StatusCode);
         if (resp.StatusCode == HttpStatusCode.NotFound) return false;
         resp.ThrowIfError();
         return true;
      }
      public bool DeleteByKey(ESConnection conn, String key)
      {
         if (!IndexExists) goto NOT_EXIST;

         ESMainResponse resp;
         if (KeyFieldName == null) goto NOT_EXIST; ;
         Logs.DebugLog.Log("DeleteByKey ({0})", key);
         if (KeyFieldName.Equals("_id", StringComparison.InvariantCultureIgnoreCase))
         {
            return deleteById(conn, key);
         }
         var req = createMatchQueryRequest(KeyFieldName, key);
         resp = conn.Post(UrlPart + "/_search", req);
         if (resp.StatusCode == System.Net.HttpStatusCode.NotFound) goto NOT_EXIST;
         resp.ThrowIfError();

         JArray hits = (JArray)resp.JObject.SelectToken("hits.hits", false);
         if (hits == null || hits.Count == 0) goto NOT_EXIST;

         String id = (String) ((JObject)hits[0])["_id"];
         if (id == null) goto NOT_EXIST;

         return deleteById(conn, id);

      NOT_EXIST:
         return false;
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

}
