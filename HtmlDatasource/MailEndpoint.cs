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
      private MultiSort<JObject> sortComparer;
      public readonly _StockSort SortMethod;
      public MailEndpoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         toMail = new List<JObject>();
         toMailForced = new List<JObject>();
         Limit = node.OptReadFloat("@limitperc", 1.0);

         MailAddr = node.OptReadStr("@email", null);
         MailServer = MailAddr==null ? node.OptReadStr("@server", null) : node.ReadStr("@server");
         MailSubject = node.OptReadStr("@subject", "[Koersen]");
         String force = node.OptReadStr("@force", null);
         if (force != null)
            ForceExpr = new Regex(force, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
         SortMethod = node.OptReadEnum("@sort", _StockSort.Percentage);
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

         if (toMail.Count==0)
         {
            logger.Log ("No percentages found.");
            return;
         }

         StringBuilder bldr = new StringBuilder();
         const String htmlHead = "<html><head><style>\r\n thead { font-weight: bold; }" +
            " .red {color: red; }" +
            " .left {text-align: left; }" +
            " td {padding: 0px 4px; text-align: right; }" +
            "</style></head><body><table>";
         const String htmlEnd = "</body></html>";

         bldr.Append("<table><thead><tr><td class='left'>Stock</td><td>Change</td><td>Price</td><td>Opened</td><td>Checked at</td></tr></thead>\r\n");
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
         b.AppendFormat("</tr>\r\n");
      }

      protected override IDataEndpoint CreateDataEndpoint(PipelineContext ctx, string dataName)
      {
         return new MailDataEndpoint(this);
      }
   }

   public class MailDataEndpoint : JsonEndpointBase<MailEndpoint>
   {
      int unexpected;
      private readonly double limitPerc;
      private readonly Regex forceExpr;

      public MailDataEndpoint(MailEndpoint m)
         : base(m)
      {
         limitPerc = m.Limit;
         forceExpr = m.ForceExpr;
      }

      private bool isForced(JObject obj)
      {
         if (forceExpr == null) return false;
         String name = obj.ReadStr(MailEndpoint.F_name);
         return forceExpr.IsMatch(name);
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
            if (isForced(base.accumulator))
            {
               accumulator.WriteToken(MailEndpoint.F_perc, perc);
               Endpoint.AddForMailForced(accumulator);
            } else if (Math.Abs(perc) >= limitPerc)
            {
               accumulator.WriteToken(MailEndpoint.F_perc, perc);
               Endpoint.AddForMail(accumulator);
            }
         }
         finally
         {
            Clear();
         }
      }
   }
}

