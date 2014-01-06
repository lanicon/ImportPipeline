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

namespace BeursGorilla
{
   public class MailEndpoint: EndPoint
   {
      private List<JObject> toMail;
      private Logger logger = Logs.CreateLogger("Gorilla", "MailEndPoint");
      private double limit;
      public MailEndpoint(ImportEngine engine, XmlNode node)
         : base(node)
      {
         toMail = new List<JObject>();
         limit = node.OptReadFloat("@limitperc", 1.0);
      }

      public override void Open(PipelineContext ctx)
      {
      }
      public override void Close(PipelineContext ctx, bool isError)
      {
         if (isError) return;
         if (toMail.Count==0)
         {
            logger.Log ("No percentages found.");
            return;
         }
         StringBuilder bldr = new StringBuilder();

         bldr.Append("<table><tr><td>Stock</td><td>Change</td><td>Price</td><td>Opened</td></tr>\r\n");
         for (int i=0; i<toMail.Count; i++) objToLine(bldr, toMail[i]);
         bldr.Append("</table>\r\n");
         logger.Log(bldr.ToString());
      }

      public void AddForMail(JObject obj)
      {
         toMail.Add(obj);
      }

      private void objToLine(StringBuilder b, JObject obj)
      {
         b.Append("<tr><td>");
         b.AppendFormat("{0}[{1}]</td>", obj.ReadStr("name"), obj.ReadStr("exchange"));
         b.AppendFormat("<td class='right'>{0:F2}</td>", obj.ReadDbl("perc"));
         b.AppendFormat("<td class='right'>{0:F3}</td>", obj.ReadDbl("price"));
         b.AppendFormat("<td class='right'>{0:F3}</td>", obj.ReadDbl("priceOpened"));
         b.AppendFormat("</tr>\r\n");
      }

      public override IDataEndpoint CreateDataEndPoint(string namePart2)
      {
         return new MailDataEndpoint(this);
      }
   }

   public class MailDataEndpoint : JsonEndpointBase<MailEndpoint>
   {
      int unexpected;
      double limitPerc;
      public MailDataEndpoint(MailEndpoint m)
         : base(m)
      {
         limitPerc = 1.0;
      }

      public override void Add(PipelineContext ctx)
      {
         try
         {
            double price = base.accumulator.ReadDbl("price", -1);
            double priceAtOpen = base.accumulator.ReadDbl("priceOpened", -1);
            if (price < 0 || priceAtOpen < 0)
            {
               if (unexpected == 0) Logs.ErrorLog.Log("Unexpected object: no price/priceOpened. Object=\r\n" + accumulator.ToString(Newtonsoft.Json.Formatting.Indented));
               unexpected++;
               return;
            }
            double perc = 100 * (price - priceAtOpen) / priceAtOpen;
            if (Math.Abs(perc) >= limitPerc)
            {
               accumulator.WriteToken("perc", perc);
               EndPoint.AddForMail(accumulator);
            }
         }
         finally
         {
            Clear();
         }
      }
   }
}

