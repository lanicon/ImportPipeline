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
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Reflection;

namespace Bitmanager.ImportPipeline
{
   class ActionAdmin
   {
      public readonly String Key;
      public readonly PipelineAction Action;
      public readonly int KeyLen;
      public readonly int Order;

      //Next fields are filled by the pipeline when sorting
      public int  Index;
      public int  EqualityID;
      public int  ActionsToSkipIfCond;
      public bool EqualToPrev;
      public bool IsCondition;

      public ActionAdmin(String key, int order, PipelineAction action)
      {
         this.Key = key.ToLowerInvariant();
         this.KeyLen = this.Key.Length;
         this.Order = order;
         this.Action = action;

         var cond = action as PipelineConditionAction;
         if (cond != null)
         {
            IsCondition = true;
            ActionsToSkipIfCond = cond.ActionsToSkip;
         }
      }
   }

   public class Pipeline : NamedItem, IDatasourceSink
   {
      private Dictionary<String, Object> variables;
      private StringDict<IDataEndpoint> endPointCache;

      public readonly String DefaultEndpoint;
      public readonly String DefaultConverters;
      public readonly ImportEngine ImportEngine;
      public readonly String ScriptTypeName;
      public readonly String DefaultPostProcessors;

      public Object ScriptObject { get; private set; }

      internal bool trace;
      private bool started;
      /// <summary>
      /// List of active Actions (only valid in a running pipeline)
      /// </summary>
      internal List<ActionAdmin> actions;

      /// <summary>
      /// List of defined Actions. Unmutable.
      /// </summary>
      internal List<ActionAdmin> definedActions;
      
      internal List<PipelineTemplate> templates;
      internal Logger logger;

      StringDict missed;

      public Pipeline(ImportEngine engine, XmlNode node): base(node)
      {
         ImportEngine = engine;
         logger = engine.DebugLog.Clone ("pipeline");

         ScriptTypeName = node.ReadStr("@script", null);
         DefaultConverters = node.ReadStr("@converters", null);
         DefaultPostProcessors = node.ReadStr("@postprocessors", null);
         DefaultEndpoint = node.ReadStr("@endpoint", null);
         if (DefaultEndpoint == null)
         {
            if (engine.Endpoints.Count == 1)
               DefaultEndpoint = engine.Endpoints[0].Name;
            else if (engine.Endpoints.GetByName(Name, false) != null)
               DefaultEndpoint = Name;
         }
         trace = node.ReadBool ("@trace", false);

         AdminCollection<PipelineAction> rawActions = new AdminCollection<PipelineAction>(node, "action", (x) => PipelineAction.Create(this, x), false);
         definedActions = new List<ActionAdmin>();
         for (int i = 0; i < rawActions.Count; i++)
         {
            var action = rawActions[i];
            String[] keys = action.Name.SplitStandard();
            for (int k = 0; k < keys.Length; k++)
               definedActions.Add(new ActionAdmin(keys[k], i, action));
         }
         definedActions.Sort(cbSortAction);

         var templNodes = node.SelectNodes("template");
         templates = new List<PipelineTemplate>(templNodes.Count);
         for (int i = 0; i < templNodes.Count; i++)
         {
            templates.Add (PipelineTemplate.Create (this, templNodes[i]));
         }

         Dump("");
      }

      public void SetVariable(String varName, Object value)
      {
         if (variables == null) variables = new Dictionary<string, object>();
         variables[varName.ToLowerInvariant()] = value;
      }
      public Object GetVariable(String varName)
      {
         if (variables == null) return null;
         Object ret;
         if (variables.TryGetValue(varName.ToLowerInvariant(), out ret)) return ret;
         return null;
      }
      public String GetVariableStr(String varName)
      {
         if (variables == null) return null;
         Object ret;
         if (variables.TryGetValue(varName.ToLowerInvariant(), out ret)) return ret.ToString();
         return null;
      }

      public void ClearVariables()
      {
         variables = null;
      }
      public void ClearVariables(String[] varsToClear)
      {
         if (variables==null || varsToClear==null) return;
         for (int i = 0; i < varsToClear.Length; i++)
         {
            variables.Remove(varsToClear[i]);
         }
      }

      public T CreateScriptDelegate<T>(String scriptName, XmlNode ctx=null) where T : class
      {
         if (scriptName == null) return null;
         if (ScriptObject == null)
            throw new BMNodeException(ctx, "Cannot create script [{0}]: No script specified at the pipeline.", scriptName);

         Type t = ScriptObject.GetType();

         MethodInfo mi = t.GetMethod(scriptName, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Instance);
         if (mi == null) throw new BMNodeException(ctx, "Cannot find method {0} in class {1}.", scriptName, t.FullName);
         T dlg = (T)(Object)Delegate.CreateDelegate(typeof(T), ScriptObject, mi);
         logger.Log("-- CreateScriptDelegate({0}) -> {1}.", scriptName, dlg);
         return dlg;
      }


      private static String[] splitEndpoint(String s)
      {
         if (String.IsNullOrEmpty(s)) return null;
         String[] parts = s.Split('.');
         for (int i = 0; i < parts.Length; i++)
            parts[i] = parts[i].TrimToNull();
         if (parts.Length >= 3) return parts;

         String[] parts3 = new String[3];
         Array.Copy (parts, parts3, parts.Length);
         return parts3;
      }

      public void Start(PipelineContext ctx)
      {
         if (trace) ctx.ImportFlags |= _ImportFlags.TraceValues;
         missed = new StringDict();
         if (ScriptTypeName != null) //NB: always create a new script object. Never reuse an existing one.
         {
            ScriptObject = Objects.CreateObject(ScriptTypeName, ctx);
            logger.Log("Script({0})={1}", ScriptTypeName, ScriptObject);
         }

         //Clone the list of actions and strat them
         actions = new List<ActionAdmin>(definedActions.Count);
         for (int i = 0; i < definedActions.Count; i++)
         {
            ActionAdmin act = definedActions[i];
            act.Action.Start(ctx);
            actions.Add(act);
         }
         prepareActions();

         if (endPointCache != null)
            foreach (var kvp in this.endPointCache) {
               kvp.Value.Start(ctx);
               var resolver = kvp.Value as IEndpointResolver;
               if (ctx.AdminEndpoint==null && resolver != null) ctx.AdminEndpoint = resolver.GetAdminEndpoint(ctx);
            }

         if (ctx.AdminEndpoint == null)
         {
            ctx.ImportLog.Log(_LogType.ltWarning, "Did not find an admin enpoint. This doesn't need to be an error.");
            ctx.RunAdministrations = null;
         }
         else
         {
            ctx.RunAdministrations = ctx.AdminEndpoint.LoadAdministration(ctx);
         }

         started = true;
      }

      public void Stop(PipelineContext ctx)
      {
         bool countCopied = false;
         ctx.PostProcessor = null;
         if (endPointCache != null)
         {
            foreach (var kvp in endPointCache)
            {
               var proc = kvp.Value as IPostProcessor;
               if (proc == null) continue;
               if (!countCopied)
               {
                  ctx.PostProcessed = ctx.Added;
                  ctx.Added = 0;
                  countCopied = true;
               }
               ctx.ImportLog.Log(_LogType.ltTimerStart, "Processing post-processors for endpoint '{0}'. First processor is '{1}'.", kvp.Key, proc.Name);
               proc.CallNextPostProcessor(ctx);
               ctx.ImportLog.Log(_LogType.ltTimerStop, "Processing post-processors finished");
            }
         }
         ctx.PostProcessor = null;
         ctx.MissedLog.Log();
         ctx.MissedLog.Log("Datasource [{0}] missed {1} keys.", ctx.DatasourceAdmin.Name, missed.Count);
         List<string> lines = new List<string>();
         foreach (var kvp in missed)
         {
            if (kvp.Value != null) continue; //skip templated actions
            lines.Add(String.Format(@"-- [{0}]", kvp.Key));
         }
         int offset = lines.Count;
         foreach (var kvp in missed)
         {
            if (kvp.Value == null) continue; //skip real missed
            lines.Add(String.Format(@"-- [{0}] matched by template [{1}].", kvp.Key, kvp.Value));
         }
         missed = new StringDict();
         lines.Sort(0, offset, StringComparer.InvariantCultureIgnoreCase);
         lines.Sort(offset, lines.Count - offset, StringComparer.InvariantCultureIgnoreCase);
         foreach (var line in lines) ctx.MissedLog.Log(line);


         //Optional save the administration records 
         if (ctx.AdminEndpoint != null)
         {
            var list = ctx.RunAdministrations;
            if (list == null) list = new List<RunAdministration>();
            list.Add (new RunAdministration(ctx));
            ctx.AdminEndpoint.SaveAdministration(ctx, list);
         }

         started = false;
         if (endPointCache != null)
            foreach (var kvp in this.endPointCache)
               kvp.Value.Stop(ctx);
         Dump("after import");

         endPointCache = null;
         actions = null;
      }

      private String getEndpointName(String name, DatasourceAdmin ds)
      {
         if (name != null)
         {
            if (name[0] != '.') return name;

            //Name with a dot indicates an amendment on the default name 
            String defName = getEndpointName(null, ds);
            int idx = defName.LastIndexOf('.');
            return (idx<0 ? defName : defName.Substring (0, idx)) + name;
         }
         
         name = ds.EndpointName;
         if (name != null) return name;

         if (DefaultEndpoint != null)
         {
            return DefaultEndpoint.Replace ("*", ds.Name);
         }
         return ds.Name;
      }

      public IDataEndpoint GetDataEndpoint(PipelineContext ctx, String name)
      {
         String endpointName = getEndpointName (name, ctx.DatasourceAdmin);
         IDataEndpoint ret;

         if (endPointCache == null) endPointCache = new StringDict<IDataEndpoint>();
         if (endPointCache.TryGetValue(endpointName, out ret)) return ret;

         ret = this.ImportEngine.Endpoints.GetDataEndpoint(ctx, endpointName);
         ret = wrapPostProcessors(ctx, ret, null);
         endPointCache.Add(endpointName, ret);
         if (started) ret.Start(ctx); 
         return ret;
      }

      //Optional wraps an existing endpoint with a set of post-processors
      private IDataEndpoint wrapPostProcessors (PipelineContext ctx, IDataEndpoint ep, String processors)
      {
         if (processors == null) processors = DefaultPostProcessors;
         if (String.IsNullOrEmpty(processors)) return ep;

         String[] arr = processors.SplitStandard();
         if (arr.Length == 0) return ep;

         IDataEndpoint wrapped = ep;
         //Warning: always wrap from the back to the front! This will lead to String results in case of duplicate postprocessors:
         //         the last one has the lowest instanceId. But it is the right way! 
         for (int i=arr.Length-1; i>=0; i--)
         {
            ctx.PostProcessor = ctx.ImportEngine.PostProcessors.GetPostProcessor(arr[i]);
            wrapped = (IDataEndpoint)ctx.PostProcessor.Clone(ctx, wrapped); 
         }
         ctx.PostProcessor = null;
         return wrapped;
      }

      /// <summary>
      /// Check if we don't have unresolved action endpoints
      /// </summary>
      public void CheckEndpoints(PipelineContext ctx, DatasourceAdmin ds)
      {
         if (ds.EndpointName != null) return;
         if (DefaultEndpoint == null || DefaultEndpoint.IndexOf ('*') >= 0)
         {
            if (definedActions.Any (a=>!a.Action.HasEndpointName))
               ctx.ImportEngine.Endpoints.CheckDataEndpoint(ctx, getEndpointName (null, ds), true); 
         }
      }


      public Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         Object orgValue = value;
         Object ret = null;
         Object lastAction = null;
         try
         {
            ctx.ActionFlags = 0;

            if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0) logger.Log("HandleValue ({0}, {1} [{2}]", key, value, value == null ? "null" : value.GetType().Name);

            if (key == null) goto EXIT_RTN;
            String lcKey = key.ToLowerInvariant();
            int keyLen = lcKey.Length;

            if (ctx.SkipUntilKey != null)
            {
               ctx.ActionFlags |= _ActionFlags.Skipped;
               if (ctx.SkipUntilKey.Length == keyLen && lcKey.Equals(ctx.SkipUntilKey, StringComparison.OrdinalIgnoreCase))
                  ctx.SkipUntilKey = null;
               goto EXIT_RTN;
            }

            int ixStart = findAction(lcKey);
            if (ixStart < 0)
            {
               if (templates.Count == 0 || !checkTemplates(ctx, key, ref lastAction)) //templates==0: otherwise checkTemplates() inserts a NOP action...
               {
                  missed[lcKey] = null;
                  goto EXIT_RTN;
               }
               ixStart = findAction(lcKey);
               if (ixStart < 0) goto EXIT_RTN;  //Should not happen, just to be sure!
            }

            for (int i = ixStart; i < actions.Count; i++)
            {
               ActionAdmin a = actions[i];
               if (i > ixStart && !a.EqualToPrev) break;

               lastAction = ctx.SetAction(a.Action);
               Object tmp = a.Action.HandleValue(ctx, key, value);
               ClearVariables(a.Action.VarsToClear);
               if (tmp != null) ret = tmp;
               if ((ctx.ActionFlags & (_ActionFlags.SkipRest | _ActionFlags.ConditionMatched) ) != 0)
               {
                  if ((ctx.ActionFlags & _ActionFlags.ConditionMatched) != 0)
                  {
                     if (!a.IsCondition) throw new BMException("Action [{0}] is not a condition.", a.Key);
                     i += a.ActionsToSkipIfCond;
                     continue;
                  }
                  break;
               }
            }

            //Make sure the skipUntil can also be set from the last action in a chain...
            if (ctx.SkipUntilKey != null && ctx.SkipUntilKey.Length == keyLen && lcKey.Equals(ctx.SkipUntilKey, StringComparison.OrdinalIgnoreCase))
               ctx.SkipUntilKey = null;

            EXIT_RTN: return ret;
         }
         catch (Exception e)
         {
            String type;
            if (orgValue == value)
               type = String.Format("[{0}]", getType(orgValue));
            else
               type = String.Format("[{0}] (was [{1}])", getType(value), getType(orgValue));
            ctx.ErrorLog.Log("Exception while handling event. Key={0}, value type={1}, action={2}", key, type, lastAction);
            ctx.ErrorLog.Log("-- value={0}", value);
            if (orgValue != value)
               ctx.ErrorLog.Log("-- orgvalue={0}", orgValue);

            ctx.ErrorLog.Log(e);
            PipelineAction act = lastAction as PipelineAction;
            if (act == null)
               ctx.ErrorLog.Log("Cannot dump accu: no current action found.");
            else
            {
               var accu = (JObject)act.Endpoint.GetFieldAsToken(null);
               ctx.ErrorLog.Log("Dumping content of current accu: fieldcount={0}", accu.Count);
               String content = accu.ToString();
               if (content != null && content.Length > 1000) content = content.Substring(0, 1000) + "...";
               ctx.ErrorLog.Log(content);
            }
            if (MaxAddsExceededException.ContainsMaxAddsExceededException(e))
               throw;


            throw new BMException (e, "{0}\r\nKey={1}, valueType={2}.", e.Message, key, type);
         }
      }

      private static String getType(Object obj)
      {
         if (obj == null) return "null";
         JValue jv = obj as JValue;
         return jv == null ? obj.GetType().Name : jv.Type.ToString();
      }

      Dictionary<String, ActionAdmin> actionDict; 
      private void prepareActions()
      {
         actionDict = new Dictionary<string, ActionAdmin>(actions.Count);
         if (actions.Count==0) return;
         int equalityID = 0;
         actions.Sort(cbSortAction);
         ActionAdmin prev = actions[0];
         actionDict.Add(prev.Key, prev);
         prev.EqualToPrev = false;
         prev.Index       = 0;
         prev.EqualityID = equalityID;
         for (int i=1; i<actions.Count; i++)
         {
            ActionAdmin a = actions[i];
            a.Index = i;
            a.EqualityID = equalityID;
            a.EqualToPrev = (a.Key == prev.Key);
            if (a.EqualToPrev) continue;
            prev = a;
            a.EqualityID = ++equalityID;
            actionDict.Add(a.Key, a);
         }
      }

      private bool checkTemplates(PipelineContext ctx, String key, ref Object lastTemplate)
      {
         PipelineAction a = null;
         String templateExpr = null;
         int i;
         for (i = 0; i < templates.Count; i++)
         {
            lastTemplate = templates[i];
            a = templates[i].OptCreateAction(ctx, key);
            if (a != null) goto ADD_TEMPLATE;
         }
         a = new PipelineNopAction (key);
         actions.Add(new ActionAdmin(a.Name, actions.Count, a));
         prepareActions();
         return false;

      ADD_TEMPLATE:
         templateExpr = templates[i].Expr;
         missed[key] = templateExpr;
         while (true)
         {
            a.Start(ctx);
            actions.Add(new ActionAdmin(a.Name, actions.Count, a));
            i++;
            if (i >= templates.Count) break;
            if (!templates[i].Expr.Equals(templateExpr, StringComparison.InvariantCultureIgnoreCase)) break;
            a = templates[i].OptCreateAction(ctx, key);
            if (a == null) break;
         }
         prepareActions();
         return true;
      }

      private int findAction(String key)
      {
         int kl = key.Length;
         for (int i = 0; i < actions.Count; i++)
         {
            ActionAdmin a = actions[i];
            if (a.KeyLen < kl) continue;
            if (a.KeyLen > kl) return -1;
            int rc = String.CompareOrdinal(a.Key, key);
            if (rc < 0) continue;
            if (rc > 0) return -1;

            return i;
         }
         return -1;
      }

      private static int cbSortAction(ActionAdmin left, ActionAdmin right)
      {
         var intComparer = Comparer<int>.Default;
         int rc = intComparer.Compare(left.KeyLen, right.KeyLen);
         if (rc != 0) return rc;

         rc = String.CompareOrdinal(left.Key, right.Key);
         if (rc != 0) return rc;

         return intComparer.Compare(left.Order, right.Order);
      }


      public void Dump (String why)
      {
         logger.Log("Dumping pipeline {0} {1}", Name, why);
         var list = actions == null ? definedActions : actions; 

         logger.Log("-- {0} actions", list.Count);
         for (int i = 0; i < list.Count; i++)
         {
            var action = list[i];
            logger.Log("-- -- action order={0} {1}", action.Order, action.Action);
         }

         logger.Log("-- {0} templates", templates.Count);
         for (int i = 0; i < templates.Count; i++)
         {
            logger.Log("-- -- " + templates[i]);
         }
      }

      public static void EmitInnerTokens(PipelineContext ctx, IDatasourceSink sink, JToken token, String key, int maxLevel)
      {
         if (token == null) return;
         maxLevel--;
         switch (token.Type)
         {
            case JTokenType.Array:
               if (maxLevel < 0) break;
               var arr = (JArray)token;
               String tmpKey = key + "/_v";
               for (int i = 0; i < arr.Count; i++)
                  EmitToken(ctx, sink, arr[i], tmpKey, maxLevel);
               sink.HandleValue(ctx, key, null);
               return;
            case JTokenType.Object:
               if (maxLevel < 0) break;
               JObject obj = (JObject)token;
               foreach (var kvp in obj)
               {
                  EmitToken(ctx, sink, kvp.Value, key + "/" + generateObjectKey(kvp.Key), maxLevel);
               }
               sink.HandleValue(ctx, key, null);
               return;
         }
      }

      public void EmitVariables (PipelineContext ctx, IDatasourceSink sink, String key, int maxLevel)
      {
         if (variables!=null)
         {
            foreach (var kvp in variables)
            {
               var tmpkey = key + '/' + kvp.Value;
               if (maxLevel <= 0) goto EMIT_RAW;

               JToken tk = kvp.Value as JToken;
               if (tk != null)
               {
                  SplitTokens(ctx, sink, tk, tmpkey, maxLevel);
                  continue;
               }

            EMIT_RAW:
               sink.HandleValue(ctx, tmpkey, kvp.Value);
            }
         }
      }

      public static void SplitInnerTokens(PipelineContext ctx, IDatasourceSink sink, JToken token, String key, int maxLevel)
      {
         if (token == null) return;
         String tmpKey;
         maxLevel--;
         switch (token.Type)
         {
            case JTokenType.Array:
               if (maxLevel < 0) break;
               var arr = (JArray)token;
               tmpKey = key + "/_v";
               for (int i = 0; i < arr.Count; i++)
                  SplitTokens(ctx, sink, arr[i], tmpKey, maxLevel);
               sink.HandleValue(ctx, key, null);
               return;
            case JTokenType.Object:
               if (maxLevel < 0) break;
               JObject obj = (JObject)token;
               tmpKey = key + '/';
               foreach (var kvp in obj)
               {
                  SplitTokens(ctx, sink, kvp.Value, tmpKey + kvp.Key, maxLevel);
               }
               sink.HandleValue(ctx, key, null);
               return;
         }
      }

      public static void SplitTokens(PipelineContext ctx, IDatasourceSink sink, JToken token, String key, int maxLevel)
      {
         if (token == null) return;
         String tmpKey;
         Object value = token;
         maxLevel--;
         switch (token.Type)
         {
            case JTokenType.None:
            case JTokenType.Null:
            case JTokenType.Undefined:
               value = null;
               break;
            case JTokenType.Date: 
            case JTokenType.String:
            case JTokenType.Float: 
            case JTokenType.Integer:
            case JTokenType.Boolean: break;

            case JTokenType.Array:
               if (maxLevel < 0) break;
               var arr = (JArray)token;
               tmpKey = key + "/_v";
               for (int i = 0; i < arr.Count; i++)
                  SplitTokens(ctx, sink, arr[i], tmpKey, maxLevel);
               sink.HandleValue(ctx, key, null);
               return;

            case JTokenType.Object:
               if (maxLevel < 0) break;
               JObject obj = (JObject)token;
               tmpKey = key + '/';
               foreach (var kvp in obj)
               {
                  SplitTokens(ctx, sink, kvp.Value, tmpKey + kvp.Key, maxLevel);
               }
               sink.HandleValue(ctx, key, null);
               return;
         }
         sink.HandleValue(ctx, key, value);
      }

      
      public static void EmitToken(PipelineContext ctx, IDatasourceSink sink, JToken token, String key, int maxLevel)
      {
         if (token == null) return;
         Object value = token;
         maxLevel--;
         switch (token.Type)
         {
            case JTokenType.Array:
               if (maxLevel < 0) break;
               var arr = (JArray)token;
               String tmpKey = key + "/_v";
               for (int i = 0; i < arr.Count; i++)
                  EmitToken(ctx, sink, arr[i], tmpKey, maxLevel);
               sink.HandleValue(ctx, key, null);
               return;
            case JTokenType.None:
            case JTokenType.Null:
            case JTokenType.Undefined:
               value = null;
               break;
            case JTokenType.Date: value = (DateTime)token; break;
            case JTokenType.String: value = (String)token; break;
            case JTokenType.Float: value = (double)token; break;
            case JTokenType.Integer: value = (Int64)token; break;
            case JTokenType.Boolean: value = (bool)token; break;

            case JTokenType.Object:
               if (maxLevel < 0) break;
               JObject obj = (JObject)token;
               foreach (var kvp in obj)
               {
                  EmitToken(ctx, sink, kvp.Value, key + "/" + generateObjectKey(kvp.Key), maxLevel);
               }
               sink.HandleValue(ctx, key, null);
               return;
         }
         sink.HandleValue(ctx, key, value);
      }
      static private String generateObjectKey(String k)
      {
         return String.IsNullOrEmpty(k) ? "_o" : k;
      }
   }




}
