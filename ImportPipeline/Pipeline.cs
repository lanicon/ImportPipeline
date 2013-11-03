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
      public readonly PipelineDataAction Action;

      public ActionAdmin(String key, int order, PipelineDataAction action)
      {
         this.KeyLen = key.Length;
         this.Key = key;
         this.Order = order;
         this.Action = action;
      }
   }

   public class Pipeline : NamedItem, IDatasourceSink
   {
      public readonly String DefaultEndPoint;
      public readonly ImportEngine ImportEngine;

      internal bool trace;
      internal List<ActionAdmin> actions;
      internal Logger logger;

      StringDict missed;

      public Pipeline(ImportEngine engine, XmlNode node): base(node)
      {
         ImportEngine = engine;
         DefaultEndPoint = node.OptReadStr("@endpoint", null);
         trace = node.OptReadBool ("@trace", false);

         AdminCollection<PipelineDataAction> rawActions = new AdminCollection<PipelineDataAction>(node, "action", (x) => PipelineDataAction.Create(this, x), true);
         actions = new List<ActionAdmin>();
         for (int i = 0; i < rawActions.Count; i++)
         {
            var action = rawActions[i];
            String[] keys = action.Name.SplitStandard();
            for (int k = 0; k < keys.Length; k++)
               actions.Add(new ActionAdmin(keys[k], i, action));
         }
         actions.Sort(cbSortAction);
         logger = Logs.CreateLogger("pipeline", Name);
         logger.Log("Dumping {0} actions", actions.Count);
         for (int i = 0; i < actions.Count; i++)
         {
            var action = actions[i];
            logger.Log ("-- action key={0}, order={1}, action={2}", action.Key, action.Order, action.Action);
         }
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

      public void HandleValue(PipelineContext ctx, String key, Object value)
      {
         if (trace) logger.Log("HandleValue ({0}, {1} ({2})", key, value, value==null ? "null": value.GetType().Name);

         if (key == null) return;
         String lcKey = key.ToLowerInvariant();
         int keyLen = lcKey.Length;
         int ixStart = findAction(lcKey);
         if (ixStart < 0)
         {
            missed[lcKey] = null;
            return;
         }

         for (int i = ixStart; i < actions.Count; i++)
         {
            ActionAdmin a = actions[i];
            if (a.KeyLen != keyLen) break;
            if (!lcKey.Equals(a.Key, StringComparison.InvariantCulture)) break;

            a.Action.HandleValue(ctx, key, value);
         }
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

   }



}
