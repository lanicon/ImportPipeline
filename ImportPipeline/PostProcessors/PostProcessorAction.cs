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
      void ProcessRecords(PipelineContext ctx, JObject[] records, int offset, int len);
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
         return new PostProcessorAddAction(processor, node);
      }

      void ProcessRecords(PipelineContext ctx, JObject[] records, int offset, int len)
      {
         foreach (var a in actions)
            a.ProcessRecords(ctx, records, offset, len);
      }

   }

   public class PostProcessorAddAction: PostProcessorNumericAction, IPostProcessorAction
   {
      public PostProcessorAddAction(IPostProcessor processor, XmlNode node): base (processor, node)
      {
      }

      public void ProcessRecords(PipelineContext ctx, JObject[] records, int offset, int len)
      {
         if (numberMode == PostProcessorNumericAction.NumberMode.Int)
            toField.WriteValue(records[offset], addLongs (records, offset, len), JEvaluateFlags.NoExceptMissing);
         else
            toField.WriteValue(records[offset], addDoubles (records, offset, len), JEvaluateFlags.NoExceptMissing);
      }
   }


   public class PostProcessorNumericAction
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



      protected long addLongs(JObject[] records, int offset, int len)
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
      protected bool maxLongs(JObject[] records, int offset, int len, out long value)
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
      protected bool minLongs(JObject[] records, int offset, int len, out long value)
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
      protected double addDoubles(JObject[] records, int offset, int len)
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
      protected bool maxDoubles(JObject[] records, int offset, int len, out double value)
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
      protected bool minDoubles(JObject[] records, int offset, int len, out double value)
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
