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

using Bitmanager.ImportPipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using Bitmanager.Core;
using System.Net.Mail;
using System.Text.RegularExpressions;

namespace BeursGorilla
{
   public enum _StockSort
   {
      Exchange =1,
      Percentage = 2,
      Name = 4,
   }

   public class MultiSort<T> : IComparer<T>
   {
      private List<Comparison<T>> comparers;
      public int Compare(T x, T y)
      {
         for (int i = 0; i < comparers.Count; i++)
         {
            int rc = comparers[i] (x,y);
            if (rc != 0) return rc;
         }
         return 0;
      }

      public MultiSort()
      {
         comparers = new List<Comparison<T>>();
      }
      public MultiSort(Comparison<T> cmp): this()
      {
         comparers.Add(cmp);
      }
      public void Add(Comparison<T> cmp)
      {
         comparers.Add(cmp);
      }
   }
 

   public class MailEndpoint: Endpoint
   {
      public const String F_price = "price";
      public const String F_priceOpened = "priceOpened";
      public const String F_perc = "perc";
      public const String F_name = "name";
      public const String F_exchange = "exchange";
      public const String F_checkTime = "checktime";

      private List<JObject> toMail, toMailForced;
      private Logger logger = Logs.CreateLogger("Gorilla", "MailEndpoint");
      public readonly double Limit;
      public readonly String MailAddr;
      public readonly String MailServer;
      public readonly String MailSubject;
      public readonly Regex ForceExpr;
      public readonly String[] TriggerAtNonempty;
      public readonly bool[] Triggered;
      private MultiSort<JObject> sortComparer;
      public readonly _StockSort SortMethod;
      public  bool debug;
      public MailEndpoint(ImportEngine engine, XmlNode node)
         : base(engine, node)
      {
         toMail = new List<JObject>();
         toMailForced = new List<JObject>();
         Limit = node.ReadFloat("@limitperc", 1.0);

         MailAddr = node.ReadStr("@email", null);
         MailServer = MailAddr==null ? node.ReadStr("@server", null) : node.ReadStr("@server");
         MailSubject = node.ReadStr("@subject", "[Koersen]");
         debug = node.ReadBool("@debug", false);
         
         String trigger = node.ReadStrRaw("@trigger-nonempty", _XmlRawMode.DefaultOnNull, "notes;note");
         TriggerAtNonempty = trigger.SplitStandard();
         if (TriggerAtNonempty.Length == 0) 
            TriggerAtNonempty = null;
         else
            Triggered = new bool[TriggerAtNonempty.Length];

         String force = node.ReadStr("@force", null);
         if (force != null)
            ForceExpr = new Regex(force, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
         SortMethod = node.ReadEnum("@sort", _StockSort.Percentage);
         sortComparer = new MultiSort<JObject>();
         if ((SortMethod & _StockSort.Exchange) != 0) sortComparer.Add((x, y) => String.CompareOrdinal((String)x["exchange"], (String)y["exchange"]));
         if ((SortMethod & _StockSort.Name) != 0) sortComparer.Add((x, y) => StringComparer.InvariantCultureIgnoreCase.Compare((String)x["name"], (String)y["name"]));
         if ((SortMethod & _StockSort.Percentage) != 0) sortComparer.Add((x, y) => Comparer<double>.Default.Compare(absolutePercentage(y), absolutePercentage(x)));
      }

      protected override void Open(PipelineContext ctx)
      {
         base.Open(ctx);
      }

      protected void buildMail(StringBuilder sb, List<JObject> stocksToMail)
      {
         stocksToMail.Sort(sortComparer);
         for (int i = 0; i < stocksToMail.Count; i++) objToLine(sb, stocksToMail[i]);
      }
      protected override void Close(PipelineContext ctx)
      {
         if (!logCloseAndCheckForNormalClose(ctx)) return;

         if (toMail.Count == 0 && toMailForced.Count==0)
         {
            logger.Log ("No filtered items to mail found.");
            return;
         }

         //Create header
         StringBuilder bldr = new StringBuilder();
         const String htmlHead = "<html><head><style>\r\n thead { font-weight: bold; }" +
            " .red {color: red; }" +
            " .left {text-align: left; }" +
            " td {padding: 0px 4px; text-align: right; }" +
            "</style></head><body><table>";
         const String htmlEnd = "</body></html>";

         bldr.Append("<table><thead><tr><td class='left'>Stock</td><td>Change</td><td>Price</td><td>Opened</td><td>Checked at</td>");
         //Append dynamic fields
         if (Triggered != null)
         {
            for (int i=0; i<Triggered.Length; i++)
            {
               if (!Triggered[i]) continue;
               bldr.Append("<td class='left'>");
               String h = TriggerAtNonempty[i];
               bldr.Append(char.ToUpperInvariant(h[0]));
               bldr.Append(h, 1, h.Length - 1);
               bldr.Append("</td>");
            }
         }
         bldr.Append("</tr></thead>\r\n");

         //Append forced(first) and non-forced lines
         buildMail(bldr, toMailForced);
         bldr.Append ("<tr></tr>");
         buildMail(bldr, toMail);
         bldr.Append("</table>\r\n");
         logger.Log(bldr.ToString());

         if (MailAddr == null) goto EXIT_RTN;

         MailAddress toMailAddr = new MailAddress(MailAddr);
         MailAddress fromMailAddr = new MailAddress("pweerd@Bitmanager.nl");
         String [] arr = MailServer.Split (':');
         int port = 25;
         if (arr.Length > 1) port = Invariant.ToInt32 (arr[1]);
         using (SmtpClient smtp = new SmtpClient(arr[0], port))
         {
            using (MailMessage m = new MailMessage())
            {
               logger.Log("Sending mail to {0}...", MailAddr);
               m.From = fromMailAddr;
               m.To.Add(toMailAddr);
               m.Subject = MailSubject;
               m.SubjectEncoding = Encoding.UTF8;
               m.BodyEncoding = Encoding.UTF8;
               m.Body = htmlHead + bldr.ToString() + htmlEnd;
               m.IsBodyHtml = true;

               //msg.Attachments.
               smtp.Send(m);
            }
         }
      EXIT_RTN:
         logCloseDone(ctx);
         base.Close(ctx);

      }
      private static double absolutePercentage(JObject obj)
      {
         return Math.Abs((double)obj.GetValue(F_perc));
      }

      public void AddForMail(JObject obj)
      {
         toMail.Add(obj);
      }
      public void AddForMailForced(JObject obj)
      {
         toMailForced.Add(obj);
      }

      private void objToLine(StringBuilder b, JObject obj)
      {
         double perc = obj.ReadDbl(F_perc);
         //b.Append ((perc < 0.0) ? "<tr class='red'>" : "<tr>");
         b.Append("<tr>");
         b.Append("<td class='left'>");
         b.AppendFormat("{0}[{1}]</td>", obj.ReadStr(F_name), obj.ReadStr(F_exchange));
         b.AppendFormat("<td {1}>{0:F2}%</td>", perc, perc < 0.0 ? " class='red'" : String.Empty );
         b.AppendFormat("<td>{0:F3}</td>", obj.ReadDbl(F_price));
         b.AppendFormat("<td>{0:F3}</td>", obj.ReadDbl(F_priceOpened));
         b.AppendFormat("<td>{0}</td>", obj.ReadStr(F_checkTime));

         //Append dynamic fields
         if (Triggered != null)
         {
            for (int i = 0; i < Triggered.Length; i++)
            {
               if (!Triggered[i]) continue;
               b.Append("<td class='left'>");
               b.Append(obj.ReadStr(TriggerAtNonempty[i]));
               b.Append("</td>");
            }
         }
         b.AppendFormat("</tr>\r\n");
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName, bool mustExcept)
      {
         return new MailDataEndpoint(this);
      }
   }

   public class MailDataEndpoint : JsonEndpointBase<MailEndpoint>
   {
      int unexpected;
      private readonly double limitPerc;
      private readonly Regex forceExpr;
      private readonly String[] triggerAtNonempty;
      private readonly bool[] triggered;
      private readonly bool debug;


      public MailDataEndpoint(MailEndpoint m)
         : base(m)
      {
         limitPerc = m.Limit;
         forceExpr = m.ForceExpr;
         triggerAtNonempty = m.TriggerAtNonempty;
         triggered = m.Triggered;
         debug = m.debug;
      }

      private bool isForced(JObject obj)
      {
         //Need to check triggerAtNonempty first: otherwise the triggered array is not correctly filled.
         if (triggerAtNonempty != null)
         {
            for (int i = 0; i < triggerAtNonempty.Length; i++)
            {
               String nm = triggerAtNonempty[i];
               JToken jt = obj[nm];
               if (jt==null) continue;
               switch (jt.Type)
               {
                  case JTokenType.Null:
                  case JTokenType.Undefined:
                  case JTokenType.None:
                     continue;
                  case JTokenType.String:
                     if (String.IsNullOrEmpty((String)jt)) continue;
                     triggered[i] = true;
                     return true;
                  default:
                     return true;
               }
            }
         }

         if (forceExpr != null)
         {
            String name = obj.ReadStr(MailEndpoint.F_name);
            if (forceExpr.IsMatch(name)) return true;
         }

         return false;
      }
      public override void Add(PipelineContext ctx)
      {
         try
         {
            double price = base.accumulator.ReadDbl(MailEndpoint.F_price, -1);
            double priceAtOpen = base.accumulator.ReadDbl(MailEndpoint.F_priceOpened, -1);
            if (price < 0 || priceAtOpen < 0)
            {
               if (unexpected == 0) Logs.ErrorLog.Log("Unexpected object: no price/priceOpened. Object=\r\n" + accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
               unexpected++;
               return;
            }
            double perc = 100 * (price - priceAtOpen) / priceAtOpen;
            if (debug) ctx.DebugLog.Log("perc={0}, rec={1}", perc, base.accumulator);
            if (isForced(base.accumulator))
            {
               if (debug) ctx.DebugLog.Log("-- forced to output");
               accumulator.WriteToken(MailEndpoint.F_perc, perc);
               Endpoint.AddForMailForced(accumulator);
            } else if (Math.Abs(perc) >= limitPerc)
            {
               if (debug) ctx.DebugLog.Log("-- perc > limit({0}", limitPerc);
               accumulator.WriteToken(MailEndpoint.F_perc, perc);
               Endpoint.AddForMail(accumulator);
            }
            else
               if (debug) ctx.DebugLog.Log("-- skipped");
         }
         finally
         {
            Clear();
         }
      }
   }
}

