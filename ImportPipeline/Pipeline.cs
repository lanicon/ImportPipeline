using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;

namespace Bitmanager.ImportPipeline
{
   class ActionAdmin
   {
      public readonly String Key;
      public readonly int KeyLen;
      public readonly int Order;
      public readonly PipelineAction Action;

      public ActionAdmin(String key, int order, PipelineAction action)
      {
         this.Key = key.ToLowerInvariant();
         this.KeyLen = this.Key.Length;
         this.Order = order;
         this.Action = action;
      }
   }

   public class Pipeline : NamedItem, IDatasourceSink
   {
      private Dictionary<String, Object> variables;
      public readonly String DefaultEndPoint;
      public readonly ImportEngine ImportEngine;
      public static readonly Object Handled = new Object();

      internal bool trace;
      internal List<ActionAdmin> actions;
      internal List<PipelineTemplate> templates;
      internal Logger logger;

      StringDict missed;
      private Object lastAction;
      public Object LastAction { get { return lastAction;}}

      public Pipeline(ImportEngine engine, XmlNode node): base(node)
      {
         ImportEngine = engine;
         logger = Logs.CreateLogger("pipeline", Name);

         DefaultEndPoint = node.OptReadStr("@endpoint", null);
         if (DefaultEndPoint == null && engine.EndPoints.Count > 0)
            DefaultEndPoint = engine.EndPoints[0].Name;

         trace = node.OptReadBool ("@trace", false);

         AdminCollection<PipelineAction> rawActions = new AdminCollection<PipelineAction>(node, "action", (x) => PipelineAction.Create(this, x), true);
         actions = new List<ActionAdmin>();
         for (int i = 0; i < rawActions.Count; i++)
         {
            var action = rawActions[i];
            String[] keys = action.Name.SplitStandard();
            for (int k = 0; k < keys.Length; k++)
               actions.Add(new ActionAdmin(keys[k], i, action));
         }
         actions.Sort(cbSortAction);

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
         return (variables == null) ? null : variables[varName.ToLowerInvariant()];
      }

      public void ClearVariables()
      {
         variables = null;
      }

      private static String[] splitEndPoint(String s)
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

      public void Start(DatasourceAdmin datasource)
      {
         logger.Log("Starting datasource {0}", datasource.Name);
         missed = new StringDict();
      }
      public void Stop(DatasourceAdmin datasource)
      {
         logger.Log("Stopped datasource {0}. {1} missed keys.", datasource.Name, missed.Count);
         foreach (var kvp in missed)
         {
            logger.Log("-- {0}", kvp.Key);
         }
         missed = new StringDict();
      }

      public Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         Object ret = null;
         lastAction = null;
         trace = true;
         if (trace) logger.Log("HandleValue ({0}, {1} ({2})", key, value, value==null ? "null": value.GetType().Name);

         if (key == null) goto UNHANDLED;
         String lcKey = key.ToLowerInvariant();
         int keyLen = lcKey.Length;
         int ixStart = findAction(lcKey);
         if (ixStart < 0)
         {
            if (!checkTemplates(ctx, key))
            {
               missed[lcKey] = null;
               goto UNHANDLED; 
            }
            ixStart = findAction(lcKey);
            if (ixStart < 0) goto UNHANDLED; ;  //Should not happen, just to be sure!
         }

         for (int i = ixStart; i < actions.Count; i++)
         {
            ActionAdmin a = actions[i];
            if (a.KeyLen != keyLen) break;
            if (!lcKey.Equals(a.Key, StringComparison.InvariantCulture)) break;

            lastAction = a.Action;
            Object tmp = a.Action.HandleValue(ctx, key, value);
            if (tmp != null) ret = tmp;
         }
         return ret == null ? Handled : ret;
         
         UNHANDLED: return null;
      }

      private bool checkTemplates(PipelineContext ctx, String key)
      {
         PipelineAction a = null;
         String templateExpr = null;
         int i;
         for (i = 0; i < templates.Count; i++)
         {
            lastAction = templates[i];
            a = templates[i].OptCreateAction(ctx, key);
            if (a != null) goto ADD_TEMPLATE;
         }
         a = new PipelineNopAction (key);
         actions.Add(new ActionAdmin(a.Name, actions.Count, a));
         actions.Sort(cbSortAction);
         return false;

      ADD_TEMPLATE:
         templateExpr = templates[i].Expr;
         actions.Add(new ActionAdmin(a.Name, actions.Count, a));
         for (i++; i < templates.Count; i++)
         {
            if (!templates[i].Expr.Equals(templateExpr, StringComparison.InvariantCultureIgnoreCase)) break;
            a = templates[i].OptCreateAction(ctx, key);
            if (a == null) break;
            actions.Add(new ActionAdmin(a.Name, actions.Count, a));
         }
         actions.Sort(cbSortAction);

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
            int rc = String.Compare(a.Key, key, StringComparison.InvariantCulture);
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

         rc = String.Compare(left.Key, right.Key, StringComparison.InvariantCultureIgnoreCase);
         if (rc != 0) return rc;

         return intComparer.Compare(left.Order, right.Order);
      }


      public void Dump (String why)
      {
         logger.Log("Dumping pipeline {0} {1}", Name, why);
         logger.Log("{0} actions", actions.Count);
         for (int i = 0; i < actions.Count; i++)
         {
            var action = actions[i];
            logger.Log ("-- action order={0} {1}", action.Order, action.Action);
         }

         logger.Log("{0} templates", templates.Count);
         for (int i = 0; i < templates.Count; i++)
         {
            logger.Log("-- " + templates[i]);
         }
      }

      public bool HandleException(PipelineContext ctx, string prefix, Exception err)
      {
         String pfx = String.IsNullOrEmpty(prefix) ? "_error" : prefix + "/_error";
         HandleValue(ctx, pfx + "/object", err);
         HandleValue(ctx, pfx + "/msg", err.Message);
         HandleValue(ctx, pfx + "/trace", err.StackTrace);
         return (HandleValue(ctx, pfx, null) != null);
      }

      public static void EmitToken(PipelineContext ctx, IDatasourceSink sink, JToken token, String key, int maxLevel)
      {
         if (token == null) return;
         Object value = token;
         maxLevel--; 
         switch (token.Type)
         {
            case JTokenType.Array:
               if (maxLevel <= 0) break;
               var arr = (JArray)token;
               for (int i=0; i<arr.Count; i++)
                  EmitToken(ctx, sink, arr[i], key, maxLevel);
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
               if (maxLevel <= 0) break;
               JObject obj = (JObject)token;
               int newLvl = maxLevel - 1;
               foreach (var kvp in obj)
               {
                  EmitToken (ctx, sink, kvp.Value, key + "/" + kvp.Key, maxLevel);
               }
               return;
         }
         sink.HandleValue(ctx, key, value);
      }
   }



}
