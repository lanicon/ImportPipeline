using Bitmanager.Core;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Xml;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   [Flags]
   public enum _ActionFlags 
   {
      None=0,
      Skip = 1<<0,
      SkipRest = 1<<2,
      SkipAll = Skip | SkipRest,
      Handled = 1<<8,
      Skipped = 1<<9,
   };

   public class PipelineContext
   {
      public readonly ImportEngine ImportEngine;
      public readonly Pipeline Pipeline;
      public readonly DatasourceAdmin DatasourceAdmin;
      public readonly Logger ImportLog;
      public readonly Logger DebugLog;
      public readonly Logger ErrorLog;
      public readonly Logger MissedLog;
      public PipelineAction Action;
      public String SkipUntilKey;
      public int Added, Deleted, Skipped;
      public _ImportFlags Flags;
      public _ActionFlags ActionFlags;

      public PipelineContext(ImportEngine eng, DatasourceAdmin ds)
      {
         ImportEngine = eng;
         DatasourceAdmin = ds;
         Pipeline = ds.Pipeline;
         ImportLog = eng.ImportLog.Clone (ds.Name);
         DebugLog = eng.DebugLog.Clone(ds.Name);
         ErrorLog = eng.ErrorLog.Clone(ds.Name);
         MissedLog = eng.MissedLog.Clone(ds.Name);
         Flags = eng.ImportFlags;
      }
      public PipelineContext(ImportEngine eng)
      {
         ImportEngine = eng;
         ImportLog = eng.ImportLog;
         DebugLog = eng.DebugLog;
         ErrorLog = eng.ErrorLog;
         MissedLog = eng.MissedLog;
         Flags = eng.ImportFlags;
      }

      internal PipelineAction SetAction(PipelineAction act)
      {
         ActionFlags |= _ActionFlags.Handled;
         Action = act;
         return act;
      }
      public void ClearEndpointAndSetFlags(_ActionFlags fl)
      {
         ActionFlags |= fl;
         Action.EndPoint.Clear();
      }
      public void ClearEndpointAndSetFlags(_ActionFlags fl, String skipUntilKey)
      {
         ActionFlags |= fl;
         Action.EndPoint.Clear();
         SkipUntilKey = skipUntilKey;
      }

      public IDatasourceFeeder CreateFeeder(XmlNode node, String expr)
      {
         String p = node.ReadStr(expr);
         IDatasourceFeeder feeder = ImportEngine.CreateObject<IDatasourceFeeder>(p);
         feeder.Init(this, node);
         return feeder;
      }

      public IDatasourceFeeder CreateFeeder(XmlNode node)
      {
         String type = node.OptReadStr("@provider", null);
         if (type == null)
         {
            XmlNode child = node.SelectSingleNode("provider");
            if (child != null) return CreateFeeder(child, "@type");
         }
         return CreateFeeder(node, "@provider");
      }

      public String GetStats()
      {
         return String.Format("Added={0}, Deleted={1}, Skipped={2}", Added, Deleted, Skipped);
      }
   }
}
