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
using System.IO;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline
{
   //public enum ExistState { NotExist = 0, Exist = 1, ExistOlder = 2, ExistSame = 4, ExistNewer = 8 };
   public delegate JObject OnLoadConfig(ESIndexDefinition index, String fn, out DateTime fileUtcDate);
   public class ESIndexDefinitions : IEnumerable<ESIndexDefinition>
   {
      private List<ESIndexDefinition> list;
      private StringDict<ESIndexDefinition> dict;

      public ESIndexDefinitions(XmlNode node)
         : this(null, node)
      {
      }
      public ESIndexDefinitions(XmlHelper xml, XmlNode node, OnLoadConfig onLoadConfig=null)
      {
         XmlNodeList nodes = node.SelectMandatoryNodes("index");
         list = new List<ESIndexDefinition>(nodes.Count);
         dict = new StringDict<ESIndexDefinition>(nodes.Count);

         foreach (XmlNode x in nodes)
         {
            ESIndexDefinition def = new ESIndexDefinition(xml, x, onLoadConfig);
            list.Add(def);
            dict.Add(def.Name, def);
         }
      }

      public int Count { get { return list.Count; } }
      public ESIndexDefinition this[int index] { get { return list[index]; } }

      public void MarkActive(bool activeValue)
      {
         for (int i = 0; i < list.Count; i++) list[i].Active = activeValue;
      }
      public void CreateIndexes(ESConnection conn, ESIndexCmd._CheckIndexFlags flags)
      {
         for (int i = 0; i < list.Count; i++) list[i].Create(conn, flags);
      }

      public void OptionalRename(ESConnection conn, int defaultGenerationsToKeep = 2)
      {
         for (int i = 0; i < list.Count; i++) list[i].OptionalRename(conn);
      }
      public void OptionalOptimize(ESConnection conn)
      {
         for (int i = 0; i < list.Count; i++) list[i].OptionalOptimize(conn);
      }
      public void Flush(ESConnection conn)
      {
         for (int i = 0; i < list.Count; i++) list[i].Flush(conn);
      }
      public void PrepareClose (ESConnection conn)
      {
         for (int i = 0; i < list.Count; i++) list[i].PrepareClose(conn);
      }


      public ESIndexDefinition GetDefinition(String name, bool mustExcept = true)
      {
         ESIndexDefinition x;
         if (name != null && dict.TryGetValue(name, out x)) return x;

         if (!mustExcept) return null;
         throw new BMException("Index '{0}' is not defined in the config.", name);
      }

      public IEnumerator<ESIndexDefinition> GetEnumerator()
      {
         return list.GetEnumerator();
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return list.GetEnumerator();
      }
   }

   public class ESIndexDefinition
   {
      private readonly OnLoadConfig onLoadConfig; 

      /// <summary>
      /// Custom state to be controlled by the caller
      /// </summary>
      public Object Custom;
      /// <summary>
      /// Open state need to be controlled by the caller
      /// </summary>
      public bool IsOpen;
      /// <summary>
      /// Determines wether the index is a definition only or that it can be used
      /// </summary>
      public bool Active;
      /// <summary>
      /// Determines wether the index is readonly
      /// </summary>
      public bool ReadOnly;
      /// <summary>
      /// Logical name of the index (not the ES name, which is IndexName)
      /// </summary>
      public readonly String Name;

      /// <summary>
      /// Generic name of the index. This could be an alias. See also IndexName 
      /// </summary>
      public readonly String AliasName;

      /// <summary>
      /// Actual name. If there is no alias, IndexName and AliasName are the same. Otherwise the AliasName is the name of the alias
      /// </summary>
      public String IndexName { get; private set; }

      public String ConfigFile { get; private set; }
      public String IndexDateTimeFormat { get; private set; }
      public String RefreshIntervalDuringImport { get; private set; }
      public String AutoTimestampFieldName { get; private set; }
      private String savedRefreshInterval;

      /// <summary>
      /// If NumShardsOnCreate > 0: override the #shards during the creation of the index
      /// </summary>
      public int NumShardsOnCreate;
      /// <summary>
      /// If NumReplicasAfterIndexed > 0: override the #replicas after the indexing-process is done
      /// </summary>
      public int NumReplicasAfterIndexed;

      public int Generations;

      /// <summary>
      /// If OptimizeToSegments > 0: do an optimize with max_num_segments=&lt;OptimizeToSegments&gt;
      /// </summary>
      public int OptimizeToSegments;
      /// <summary>
      /// If OptimizeWait > 0: a wait will be done for max of timeout ms
      /// </summary>
      public int OptimizeWait;

      public List<String> DocMappings { get; private set; }
      public List<ESIndexDocType> DocTypes { get; private set; }

      public bool IsNewIndex { get; private set; }
      public bool RenameNeeded { get; private set; }
      public bool IndexExists { get; private set; }


      private ESIndexDefinition(XmlNode node, OnLoadConfig onLoadConfig)
      {
         this.onLoadConfig = onLoadConfig == null ? _defOnLoadConfig : onLoadConfig;
         Active = node.ReadBool("@active", true);
         ReadOnly = node.ReadBool("@readonly", false);
         Name = node.ReadStr("@name");
         IndexName = node.ReadStr("@indexname", Name);
         AliasName = IndexName;
         NumShardsOnCreate = node.ReadInt("@shards", -1);
         NumReplicasAfterIndexed = node.ReadInt("@replicas", -1);
         OptimizeToSegments = node.ReadInt("@optimize_segments", 5);
         OptimizeWait = node.ReadInt("@optimize_wait", 300000); //Default: 5 minutes
         AutoTimestampFieldName = node.ReadStr("@refreshinterval", null);
         RefreshIntervalDuringImport = node.ReadStr("@refreshinterval", null);
         ConfigFile = node.ReadStrRaw("@config", _XmlRawMode.ExceptNullValue | _XmlRawMode.EmptyToNull);
         IndexDateTimeFormat = node.ReadStrRaw("@indexname_dateformat", _XmlRawMode.DefaultOnNull | _XmlRawMode.EmptyToNull, ESIndexCmd.DEFAULT_DATETIMEFORMAT);
         Generations = node.ReadInt("@generations", IndexDateTimeFormat==null ? 0 : 2);
         if (Generations != 0 && IndexDateTimeFormat == null)
            throw new BMNodeException(node, "Cannot have generations without indexname_dateformat.");
      }
      public ESIndexDefinition(XmlHelper xml, XmlNode node, OnLoadConfig onLoadConfig)
         : this(node, onLoadConfig)
      {
         DocTypes = loadDocTypesFromNode(node);
         if (ConfigFile != null)
            ConfigFile = xml.CombinePath(ConfigFile);
      }
      private static JObject _defOnLoadConfig(ESIndexDefinition index, String fn, out DateTime fileUtcDate)
      {
         if (fn==null)
         {
            fileUtcDate = DateTime.MinValue;
            return null;
         }
         fileUtcDate = File.GetLastWriteTimeUtc(fn);
         return JObject.Parse(IOUtils.LoadFromFile(fn));
      }


      private List<ESIndexDocType> loadDocTypesFromNode(XmlNode node)
      {
         var ret = new List<ESIndexDocType>();
         XmlNodeList list = node.SelectNodes("type");
         if (list.Count == 0) return ret;

         for (int i = 0; i < list.Count; i++)
            ret.Add(new ESIndexDocType(this, list[i]));
         return ret;
      }

      public String GetPathForUrl(String type)
      {
         return IndexName + "/" + type;
      }
      public String GetPathForUrl(String mapping, bool exceptIfNotFound)
      {
         if (!DocMappings.Contains(mapping))
         {
            if (exceptIfNotFound) throw new BMException("Mapping '{0}' not found in index {1}\r\nPossible mappings: {2}.",
               mapping, AliasName, String.Join(",", DocMappings.ToArray()));
         }
         return IndexName + "/" + mapping;
      }

      public override string ToString()
      {
         return String.Format("IndexDefinition ({0}): override #shards={1}, #repl={2}", AliasName, NumShardsOnCreate, NumReplicasAfterIndexed);
      }

      private String findDefaultMappingsName()
      {
         for (int i = 0; i < DocMappings.Count; i++)
         {
            String name = DocMappings[i];
            if (name.StartsWith("_")) continue;
            if (name.EndsWith("_")) continue;
            return name;
         }
         return null;
      }

      private void overrideShardsOnCreate(ESIndexCmd cmd, JObject req)
      {
         if (this.NumShardsOnCreate > 0)
         {
            req.WriteToken("settings.number_of_shards", NumShardsOnCreate);
         }
      }


      private static void addIfNotFound(JObject config, String name, String type, JToken mustIndex, String analyzer = null)
      {
         if (config[name] != null) return;
         JObject x = new JObject();
         x["type"] = type;
         x["index"] = mustIndex;
         if (analyzer != null) x["analyzer"] = analyzer;
         config[name] = x;
      }
      private static bool disableAll (JObject config)
      {
         const String ALL = "_all";
         if (config[ALL] != null) return false;
         JObject x = new JObject();
         x["enabled"] = false;
         config[ALL] = x;
         return true;
      }

      private static bool checkProperties(JObject mappings, String name)
      {
         JToken tk = mappings[name];
         if (tk == null) return false;
         switch (tk.Type)
         {
            case JTokenType.Boolean:
            case JTokenType.String:
               break;
            default: return false;
         }
         var o = new JObject();
         o["properties"] = new JObject();
         disableAll(o);
         mappings[name] = o;
         return true;
      }

      private void patchConfig(ESConnection conn, JObject config)
      {
         if (config == null) return;

         bool v5 = conn.Version.FormattedVersion.Major >= 5;
         JToken FALSE, TRUE;
         String stringType;
         if (v5)
         {
            FALSE = false;
            TRUE = true;
            stringType = "text";
         } else {
            FALSE = "no";
            TRUE = "not_analyzed";
            stringType = "string";
         }

         const String ERRORS = "errors_";
         const String ADMIN = "admin_";
         JObject mappings = (JObject)config["mappings"];
         if (mappings == null) return;

         Logger logger = Logs.CreateLogger("import", "indexdef");

         if (checkProperties(mappings, ERRORS))
            logger.Log(_LogType.ltInfo, "ES config: mapping created for [{0}].", ERRORS);
         if (checkProperties(mappings, ADMIN))
            logger.Log(_LogType.ltInfo, "ES config: mapping created for [{0}].", ADMIN);

         foreach (var kvp in mappings)
         {
            String key = kvp.Key;
            JObject o = kvp.Value as JObject;
            if (o == null) continue;
            if (disableAll(o)) logger.Log(_LogType.ltInfo, "ES config: disabled _all for type [{0}]. If you don't want this, add '_all: {{enabled: true}}'. ", key);

            var props = (JObject)o["properties"];
            if (props == null) continue;
            switch (kvp.Key)
            {
               case ERRORS:
                  addIfNotFound(props, "err_ds", stringType, FALSE);
                  addIfNotFound(props, "err_date", "date", TRUE);
                  addIfNotFound(props, "err_key", stringType, FALSE);
                  addIfNotFound(props, "err_text", stringType, FALSE);
                  addIfNotFound(props, "err_stack", stringType, FALSE);
                  continue;
               case ADMIN:
                  addIfNotFound(props, "adm_ds", stringType, FALSE);
                  addIfNotFound(props, "adm_date", "date", TRUE);
                  addIfNotFound(props, "adm_flags", stringType, FALSE);
                  addIfNotFound(props, "adm_state", stringType, FALSE);
                  addIfNotFound(props, "adm_added", "long", FALSE);
                  addIfNotFound(props, "adm_deleted", "long", FALSE);
                  addIfNotFound(props, "adm_emitted", "long", FALSE);
                  addIfNotFound(props, "adm_errors", "long", FALSE);
                  addIfNotFound(props, "adm_skipped", "long", FALSE);
                  continue;
            }
         }
      }

      public bool Create(ESConnection conn, ESIndexCmd._CheckIndexFlags flags)
      {
         if (!Active) return false;
         if (IsOpen) return false;

         //Switch off append flags if there's nothing to append
         if (IndexDateTimeFormat == null || Generations == 0)
            flags &= ~ESIndexCmd._CheckIndexFlags.AppendDate;

         if (ReadOnly) flags |= ESIndexCmd._CheckIndexFlags.DontCreate;

         DocMappings = null;
         DateTime configDate;
         JObject configJson = onLoadConfig (this, ConfigFile, out configDate);
         patchConfig(conn, configJson);

         ESIndexCmd cmd = createIndexCmd(conn);
         cmd.OnCreate = overrideShardsOnCreate;
         bool isNew;
         IndexName = cmd.CheckIndexFromFile(AliasName, configJson, configDate, flags, out isNew);//PW naar kijken!
         IndexExists = IndexName != null;
         IsNewIndex = isNew;
         RenameNeeded = (flags & ESIndexCmd._CheckIndexFlags.AppendDate) != 0 && isNew;

         //Get possible mappings and determine default doctype
         conn.Logger.Log("");
         conn.Logger.Log("get mappings");
         DocMappings = new List<String>();

         if (IndexName != null) //Might be null if the index was not found (if Readonly==true)
         {
            ESGetMappingResponse resp = cmd.GetIndexMappings(IndexName);
            conn.Logger.Log("Index[0]={0}", resp[0].Name);
            foreach (var mapping in resp[0])
            {
               conn.Logger.Log("-- Mapping={0}", mapping.Name);
               DocMappings.Add(mapping.Name);
            }
         }

         if (RefreshIntervalDuringImport != null && !ReadOnly)
         {
            JObject curSettings = cmd.GetSettings();
            this.savedRefreshInterval = curSettings.ReadStr ("refresh_interval", "1s");
            curSettings = new JObject();
            curSettings["refresh_interval"] = RefreshIntervalDuringImport;
            cmd.PutSettings(curSettings);
            conn.Logger.Log("-- RefreshInterval changed from={0} into {1}", savedRefreshInterval, RefreshIntervalDuringImport);
         }
         IsOpen = true;
         return isNew;
      }

      private ESIndexCmd createIndexCmd (ESConnection conn)
      {
         return new ESIndexCmd(conn, this.IndexDateTimeFormat);
      }
      public void Flush(ESConnection conn)
      {
         if (!IsOpen) return;
         ESIndexCmd cmd = createIndexCmd(conn);
         cmd.Flush(IndexName);
      }
      public void PrepareClose(ESConnection conn)
      {
         if (!IsOpen) return;
         ESIndexCmd cmd = createIndexCmd(conn);
         if (savedRefreshInterval != null)
         {
            JObject curSettings = new JObject();
            curSettings["refresh_interval"] = savedRefreshInterval;
            cmd.PutSettings(curSettings);
            conn.Logger.Log("-- RefreshInterval restored to {0}", savedRefreshInterval);
         }
         cmd.Flush(IndexName);
      }

      public void OptionalOptimize(ESConnection conn)
      {
         if (OptimizeToSegments <= 0) return;
         if (!IsOpen) return;
         ESIndexCmd cmd = createIndexCmd(conn);
         cmd.Optimize(IndexName, OptimizeToSegments, OptimizeWait);
      }
      public void OptionalRename(ESConnection conn)
      {
         if (!IsOpen || !Active) return;
         IsOpen = false;
         if (!RenameNeeded) return;

         Logger logger = conn.Logger.Clone(GetType().Name); 
         logger.Log("Optional rename name={0}, alias={1}, gen={2}", IndexName, AliasName, Generations);
         ESIndexCmd cmd = createIndexCmd(conn);

         String existingIndexName;
         cmd.GetIndexMappingsAndRealName(AliasName, out existingIndexName);

         logger.Log("Optional rename alias={0}, existing={1}", AliasName, existingIndexName);

         //Check if the current index was created without a timestamp. If so, we will just remove it
         //This is more or less a backward compat. issue
         if (String.Compare(AliasName, existingIndexName, StringComparison.InvariantCultureIgnoreCase) == 0)
         {
            logger.Log("Removing index {0}", existingIndexName);
            cmd.RemoveIndex(existingIndexName);
         }

         cmd.RenameAlias(AliasName, IndexName);

         //Changing the #replica's if requested.
         if (this.NumReplicasAfterIndexed > 0)
         {
            logger.Log("Setting the #replicas for {0} to {1}", IndexName, NumReplicasAfterIndexed);
            setNumberOfReplicas(conn, IndexName, NumReplicasAfterIndexed);
         }

         if (Generations <= 0) return;
         List<String> indexes = cmd.GetIndexes(AliasName, true);

         logger.Log("Found {0} indexes starting with {1}:", indexes.Count, AliasName);
         int existingIdx = -1;
         int currentIdx = -1;
         for (int i = 0; i < indexes.Count; i++)
         {
            String s = indexes[i];
            String what = String.Empty;
            if (String.Equals(s, existingIndexName, StringComparison.InvariantCultureIgnoreCase))
            {
               existingIdx = i;
               what = " [existing alias]";
            }
            else if (String.Equals(s, IndexName, StringComparison.InvariantCultureIgnoreCase))
            {
               currentIdx = i;
               what = " [current index]";
            }
            logger.Log("__ " + s + what);
         }

         //Check if the returned list is OK (our index should be in, as well as the current alias)
         if (currentIdx < 0)
            throwNotFound(IndexName);
         if (existingIdx < 0 && existingIndexName != null)
         {
            String msg = notFoundMsg(existingIndexName);
            Logs.ErrorLog.Log(msg);
            logger.Log(_LogType.ltError, msg);
         }

         //Remove all indexes between the current one and an existing alias. These are artifacts of temp. or crashed imports
         logger.Log("Removing indexes between existing ({0}) and current ({1})...", existingIdx, currentIdx);
         for (int i = existingIdx + 1; i < currentIdx; i++)
         {
            logger.Log("Removing index {0}", indexes[i]);
            cmd.RemoveIndex(indexes[i]);
         }

         //Remove all indexes between start-of-time and the existing alias, keeping 'generationsToKeep' generations
         logger.Log("Removing indexes between epoch and existing ({0}), keeping {1} generations...", existingIdx, Generations);
         for (int i = 0; i <= existingIdx - Generations + 1; i++)
         {
            logger.Log("Removing index {0}", indexes[i]);
            cmd.RemoveIndex(indexes[i]);
         }
      }
      private void setNumberOfReplicas(ESConnection conn, String name, int replicas)
      {
         JObject obj = new JObject();
         obj.WriteToken("settings.number_of_replicas", replicas);
         conn.Put(name + "/_settings", obj).ThrowIfError();
      }
      private static String notFoundMsg (String s)
      {
         return String.Format ("OptionalRename: index '{0}' not found in the list of existing indexes.", s);
      }
      private static void throwNotFound(String s)
      {
         throw new BMException(notFoundMsg (s));
      }

   }
}
