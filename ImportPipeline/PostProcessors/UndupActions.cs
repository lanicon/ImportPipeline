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

using Bitmanager.Core;
using Bitmanager.Elastic;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
   public interface IUndupAction
   {
      IUndupAction Clone(PipelineContext ctx);
      void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len);
   }

   public abstract class UndupActionBase: IUndupAction
   {
      public virtual IUndupAction Clone(PipelineContext ctx)
      {
         return this;
      }
      public abstract void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len);
   }

   public class UndupActions
   {
      protected readonly List<IUndupAction> actions;
      public UndupActions(IPostProcessor processor, XmlNode node)
      {
         XmlNodeList nodes = node.SelectNodes("action");
         actions = new List<IUndupAction>(nodes.Count);
         foreach (XmlNode child in nodes)
         {
            actions.Add(CreateAction(processor, child));
         }
      }
      protected UndupActions(PipelineContext ctx, UndupActions other)
      {
         actions = new List<IUndupAction>(other.actions.Count);
         foreach (var a in other.actions)
            actions.Add (a.Clone(ctx));
      }

      public UndupActions Clone(PipelineContext ctx)
      {
         return new UndupActions(ctx, this);
      }

      public static IUndupAction CreateAction(IPostProcessor processor, XmlNode node)
      {
         String type = node.ReadStr("@type");
         if ("add".Equals (type, StringComparison.OrdinalIgnoreCase)) return new UndupNumericAddAction(processor, node);
         if ("max".Equals (type, StringComparison.OrdinalIgnoreCase)) return new UndupNumericMaxAction(processor, node);
         if ("min".Equals (type, StringComparison.OrdinalIgnoreCase)) return new UndupNumericMinAction(processor, node);
         if ("mean".Equals (type, StringComparison.OrdinalIgnoreCase)) return new UndupNumericMeanAction(processor, node);
         if ("count".Equals(type, StringComparison.OrdinalIgnoreCase)) return new UndupCountAction(processor, node);
         if ("script".Equals(type, StringComparison.OrdinalIgnoreCase)) return new UndupScriptAction(processor, node);
         throw new BMException("Unrecognized type [{0}] for a post process action.", type);
         //TODO make this more generic
      }

      public void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         ctx.ActionFlags = _ActionFlags.None;
         foreach (var a in actions)
            a.ProcessRecords(ctx, records, offset, len);
      }

   }

   public class UndupScriptAction: UndupActionBase
   {
      public delegate void ScriptDelegate(PipelineContext ctx, List<JObject> records, int offset, int len);
      public readonly String ScriptName;
      protected ScriptDelegate scriptDelegate;

      public UndupScriptAction(IPostProcessor processor, XmlNode node)
      {
         ScriptName = node.ReadStr("@script");
      }
      protected UndupScriptAction(PipelineContext ctx, UndupScriptAction other)
      {
         ScriptName = other.ScriptName;
         scriptDelegate = ctx.Pipeline.CreateScriptDelegate < ScriptDelegate>(ScriptName);
      }
      public override IUndupAction Clone(PipelineContext ctx)
      {
         return new UndupScriptAction(ctx, this);
      }


      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         scriptDelegate(ctx, records, offset, len);
      }
   }

   public class UndupNumericAddAction : UndupNumericNumericAction
   {
      public UndupNumericAddAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == UndupNumericNumericAction.NumberMode.Int)
            toField.WriteValue(records[offset], addLongs(records, offset, len), JEvaluateFlags.NoExceptMissing);
         else
            toField.WriteValue(records[offset], addDoubles(records, offset, len), JEvaluateFlags.NoExceptMissing);
      }
   }

   public class UndupNumericMeanAction : UndupNumericNumericAction, IUndupAction
   {
      public UndupNumericMeanAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == UndupNumericNumericAction.NumberMode.Int)
         {
            long mean = len == 0 ? 0 : addLongs(records, offset, len) / len;
            toField.WriteValue(records[offset], mean, JEvaluateFlags.NoExceptMissing);
         }
         else
         {
            double mean = len == 0 ? 0 : addDoubles(records, offset, len) / len;
            toField.WriteValue(records[offset], mean, JEvaluateFlags.NoExceptMissing);
         }
      }
   }

   public class UndupCountAction : UndupActionBase
   {
      protected JPath toField;

      public UndupCountAction(IPostProcessor processor, XmlNode node)
      {
         toField = new JPath(node.ReadStr("@tofield"));
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         toField.WriteValue(records[offset], len, JEvaluateFlags.NoExceptMissing);
      }
   }

   public class UndupNumericMaxAction : UndupNumericNumericAction, IUndupAction
   {
      long defMaxLong;
      double defMaxDouble;
      public UndupNumericMaxAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
         defMaxLong = long.MinValue;
         defMaxDouble = double.MinValue;
         if (numberMode == UndupNumericNumericAction.NumberMode.Int)
            defMaxLong = node.ReadInt64("@default", defMaxLong);
         else
            defMaxDouble = node.ReadFloat("@default", defMaxDouble);
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == UndupNumericNumericAction.NumberMode.Int)
         {
            long ml;
            if (!maxLongs(records, offset, len, out ml)) ml = defMaxLong;
            toField.WriteValue(records[offset], ml, JEvaluateFlags.NoExceptMissing);
         }
         else
         {
            double dl;
            if (!maxDoubles(records, offset, len, out dl)) dl = defMaxDouble;
            toField.WriteValue(records[offset], dl, JEvaluateFlags.NoExceptMissing);
         }
      }
   }

   public class UndupNumericMinAction : UndupNumericNumericAction, IUndupAction
   {
      long defMinLong;
      double defMinDouble;
      public UndupNumericMinAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
         defMinLong = long.MaxValue;
         defMinDouble = double.MaxValue;
         if (numberMode == UndupNumericNumericAction.NumberMode.Int)
            defMinLong = node.ReadInt64("@default", defMinLong);
         else
            defMinDouble = node.ReadFloat("@default", defMinDouble);
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == UndupNumericNumericAction.NumberMode.Int)
         {
            long ml;
            if (!minLongs(records, offset, len, out ml)) ml = defMinLong;
            toField.WriteValue(records[offset], ml, JEvaluateFlags.NoExceptMissing);
         }
         else
         {
            double dl;
            if (!minDoubles(records, offset, len, out dl)) dl = defMinDouble;
            toField.WriteValue(records[offset], dl, JEvaluateFlags.NoExceptMissing);
         }
      }
   }


   public abstract class UndupNumericNumericAction: UndupActionBase
   {
      public enum NumberMode {Float, Int};
      protected NumberMode numberMode;
      protected JPath fromField;
      protected JPath toField;

      public UndupNumericNumericAction (IPostProcessor processor, XmlNode node)
      {
         numberMode = node.ReadEnum("@number", NumberMode.Int);
         String from = node.ReadStr("field");
         fromField = new JPath(from);
         String to = node.ReadStr("tofield", null);
         toField = to == null ? fromField : new JPath(to);
      }

      protected bool TryGetField(JObject rec, out double result)
      {
         JToken tk = fromField.Evaluate(rec, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto NOT_EXIST;

         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto NOT_EXIST;
         }
         result = (double)tk;
         return true;
      NOT_EXIST:
         result = 0;
         return false;
      }

      protected bool TryGetField(JObject rec, out long result)
      {
         JToken tk = fromField.Evaluate(rec, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto NOT_EXIST;

         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto NOT_EXIST;
         }
         result = (long)tk;
         return true;
      NOT_EXIST:
         result = 0;
         return false;
      }
      protected bool TryGetField(JObject rec, out int result)
      {
         JToken tk = fromField.Evaluate(rec, JEvaluateFlags.NoExceptMissing);
         if (tk == null) goto NOT_EXIST;

         switch (tk.Type)
         {
            case JTokenType.Undefined:
            case JTokenType.Null: goto NOT_EXIST;
         }
         result = (int)tk;
         return true;
      NOT_EXIST:
         result = 0;
         return false;
      }



      protected long addLongs(List<JObject> records, int offset, int len)
      {
         long ret = 0;
         int end = offset + len;
         for (int i = offset; i < end; i++)
         {
            long x;
            if (TryGetField(records[i], out x)) ret += x;
         }
         return ret;
      }
      protected bool maxLongs(List<JObject> records, int offset, int len, out long value)
      {
         bool ret = false;
         long v = long.MinValue;
         int end = offset + len;
         for (int i = offset; i < end; i++)
         {
            long x;
            if (!TryGetField(records[i], out x)) continue;
            ret = true;
            if (x > v) v = x;
         }
         value = v;
         return ret;
      }
      protected bool minLongs(List<JObject> records, int offset, int len, out long value)
      {
         bool ret = false;
         long v = long.MaxValue;
         int end = offset + len;
         for (int i = offset; i < end; i++)
         {
            long x;
            if (!TryGetField(records[i], out x)) continue;
            ret = true;
            if (x < v) v = x;
         }
         value = v;
         return ret;
      }
      protected double addDoubles(List<JObject> records, int offset, int len)
      {
         double ret = 0;
         int end = offset + len;
         for (int i = offset; i < end; i++)
         {
            double x;
            if (TryGetField(records[i], out x)) ret += x;
         }
         return ret;
      }
      protected bool maxDoubles(List<JObject> records, int offset, int len, out double value)
      {
         bool ret = false;
         double v = double.MinValue;
         int end = offset + len;
         for (int i = offset; i < end; i++)
         {
            double x;
            if (!TryGetField(records[i], out x)) continue;
            ret = true;
            if (x > v) v = x;
         }
         value = v;
         return ret;
      }
      protected bool minDoubles(List<JObject> records, int offset, int len, out double value)
      {
         bool ret = false;
         double v = double.MaxValue;
         int end = offset + len;
         for (int i = offset; i < end; i++)
         {
            double x;
            if (!TryGetField(records[i], out x)) continue;
            ret = true;
            if (x < v) v = x;
         }
         value = v;
         return ret;
      }
   }
}
