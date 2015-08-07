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
   public interface IPostProcessorAction
   {
      void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len);
   }


   public class PostProcessorActions
   {
      enum PostProcessorActionType { Script, Add, Min, Max, Mean, Concat };
      List<IPostProcessorAction> actions;
      public PostProcessorActions(IPostProcessor processor, XmlNode node)
      {
         XmlNodeList nodes = node.SelectNodes("action");
         actions = new List<IPostProcessorAction>(nodes.Count);
         foreach (XmlNode child in nodes)
         {
            actions.Add(CreateAction(processor, child));
         }
      }

      public static IPostProcessorAction CreateAction(IPostProcessor processor, XmlNode node)
      {
         String type = node.ReadStr("@type");
         if ("add".Equals (type, StringComparison.OrdinalIgnoreCase)) return new PostProcessorAddAction(processor, node);
         if ("max".Equals (type, StringComparison.OrdinalIgnoreCase)) return new PostProcessorMaxAction(processor, node);
         if ("min".Equals (type, StringComparison.OrdinalIgnoreCase)) return new PostProcessorMinAction(processor, node);
         if ("mean".Equals (type, StringComparison.OrdinalIgnoreCase)) return new PostProcessorMeanAction(processor, node);
         if ("count".Equals (type, StringComparison.OrdinalIgnoreCase)) return new PostProcessorCountAction(processor, node);
         throw new BMException("Unrecognized type [{0}] for a post process action.", type);
         //TODO make this more generic
      }

      public void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         foreach (var a in actions)
            a.ProcessRecords(ctx, records, offset, len);
      }

   }

   public class PostProcessorAddAction : PostProcessorNumericAction, IPostProcessorAction
   {
      public PostProcessorAddAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
            toField.WriteValue(records[offset], addLongs(records, offset, len), JEvaluateFlags.NoExceptMissing);
         else
            toField.WriteValue(records[offset], addDoubles(records, offset, len), JEvaluateFlags.NoExceptMissing);
      }
   }

   public class PostProcessorMeanAction : PostProcessorNumericAction, IPostProcessorAction
   {
      public PostProcessorMeanAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
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

   public class PostProcessorCountAction : IPostProcessorAction
   {
      protected JPath toField;

      public PostProcessorCountAction(IPostProcessor processor, XmlNode node)
      {
         toField = new JPath(node.ReadStr("@tofield"));
      }

      public virtual void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         toField.WriteValue(records[offset], len, JEvaluateFlags.NoExceptMissing);
      }
   }

   public class PostProcessorMaxAction : PostProcessorNumericAction, IPostProcessorAction
   {
      long defMaxLong;
      double defMaxDouble;
      public PostProcessorMaxAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
         defMaxLong = long.MinValue;
         defMaxDouble = double.MinValue;
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
            defMaxLong = node.ReadInt64("@default", defMaxLong);
         else
            defMaxDouble = node.ReadFloat("@default", defMaxDouble);
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
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

   public class PostProcessorMinAction : PostProcessorNumericAction, IPostProcessorAction
   {
      long defMinLong;
      double defMinDouble;
      public PostProcessorMinAction(IPostProcessor processor, XmlNode node)
         : base(processor, node)
      {
         defMinLong = long.MaxValue;
         defMinDouble = double.MaxValue;
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
            defMinLong = node.ReadInt64("@default", defMinLong);
         else
            defMinDouble = node.ReadFloat("@default", defMinDouble);
      }

      public override void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len)
      {
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
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


   public abstract class PostProcessorNumericAction
   {
      public enum NumberMode {Float, Int};
      protected NumberMode numberMode;
      protected JPath fromField;
      protected JPath toField;

      public PostProcessorNumericAction (IPostProcessor processor, XmlNode node)
      {
         numberMode = node.ReadEnum("@number", NumberMode.Int);
         String from = node.ReadStr("field");
         fromField = new JPath(from);
         String to = node.ReadStr("tofield", null);
         toField = to == null ? fromField : new JPath(to);
      }

      public abstract void ProcessRecords(PipelineContext ctx, List<JObject> records, int offset, int len);

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
