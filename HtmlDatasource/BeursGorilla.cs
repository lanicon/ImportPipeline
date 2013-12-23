using Bitmanager.ImportPipeline;
using Bitmanager.Core;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;

using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Net;
using System.IO;
using HtmlAgilityPack;
using System.Text.RegularExpressions;

namespace BeursGorilla
{

   public class BeursGorillaDS : Datasource
   {
      private DateTime date;
      private IDatasourceFeeder feeder;
      private bool needHistory;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         date = DateTime.Today;
         feeder = createFeeder (ctx, node);
         needHistory = node.OptReadBool("@history", true);
      }

      public IDatasourceFeeder createFeeder(PipelineContext ctx, XmlNode node, String expr)
      {
         String p = node.ReadStr(expr);
         Object rawObj = PipelineContext.CreateObject(p);

         IDatasourceFeeder obj = rawObj as IDatasourceFeeder;
         if (obj == null)
            throw new BMNodeException(node, "Object ({0}) does not support IDatasourceContentProvider.", rawObj.GetType().FullName);
         obj.Init(ctx, node);
         return obj;
      }

      public IDatasourceFeeder createFeeder(PipelineContext ctx, XmlNode node)
      {
         String type = node.OptReadStr ("@provider", null);
         if (type==null)
         {
            XmlNode child = node.SelectSingleNode ("provider");
            if (child!= null) return createFeeder (ctx, child, "@type"); 
         }
         return createFeeder(ctx, node, "@provider");
      }

      private static double toDouble(String x)
      {
         return Invariant.ToDouble(x.Replace(',', '.'));
      }

      private HtmlDocument loadUrl(Uri uri)
      {
         using (WebClient client = new WebClient())
         {
            byte[] bytes = client.DownloadData(uri);
            MemoryStream m = new MemoryStream(bytes);
            m.Position = 0;
            HtmlDocument doc = new HtmlDocument();
            doc.Load(m, Encoding.UTF8);
            return doc;
         }
      }


      private class PricePerDate
      {
         public readonly DateTime Date;
         public readonly double Price;
         public PricePerDate(HtmlNode row)
         {
            var cols = row.SelectNodes("td");
            if (cols.Count != 2)
               throw new BMException("Unexpected column count: {0}. Expected=2.", cols.Count);

            String s = cols[0].InnerText;
            String[] arr = s.Split('-');
            if (arr.Length != 3) throw new BMException("Unexpected date: {0}.", s);
            Date = new DateTime (Invariant.ToInt32(arr[2]), Invariant.ToInt32(arr[1]), Invariant.ToInt32(arr[0]));
            Price = toDouble(cols[1].InnerText);
         }
         public JObject ToJObject()
         {
            var ret = new JObject();
            ret.WriteToken("price", Price);
            ret.WriteToken("date", Date);
            return ret;
         }
      }
      private List<PricePerDate> loadHistory(Uri baseUri, String id, String name)
      {
         List<PricePerDate> prices = new List<PricePerDate>();
         const String fmt = "/fonds-informatie.asp?naam={1}&cat=historie&symbol=&instrumentcode={0}&subcat=11";
         HtmlDocument doc = loadUrl(new Uri(baseUri, String.Format(fmt, id, name)));

         loadHistory(prices, doc, "//table[@class='fonds_info_koersen_links']");
         loadHistory(prices, doc, "//table[@class='fonds_info_koersen_rechts']");

         Logs.ErrorLog.Log (_LogType.ltWarning, "Unexpected small history: {0} days. Name={1}, Id={2}", prices.Count, name, id);
         return prices;
      }

      private static JArray toJArray(List<PricePerDate> prices)
      {
         JArray ret = new JArray();
         for (int i = 0; i < prices.Count; i++)
            ret.Add(prices[i].ToJObject());
         return ret;
      }

      static Logger posLogger = Logs.CreateLogger ("pos", "pos");
      private int computePos (List<PricePerDate> prices, DateTime minDate, String name, out int dev)
      {
         dev = 0;
         if (prices.Count < 2) return -1;
         var elt = prices[0];
         double min = elt.Price;
         double max = elt.Price;
         double price = elt.Price;
         double mean = price;
         double totDev = 0.0;

         int n = 0;
         for (int i = 1; i < prices.Count; i++)
         {
            elt = prices[i];
            if (elt.Date < minDate) break;
            n = i;
            if (elt.Price > max) max = elt.Price;
            else if (elt.Price < min) min = elt.Price;
         }
         if (max-min < 0.01) return 100;

         mean = mean / (n+1);
         for (int i = 0; i <= n; i++)
         {
            totDev += Math.Abs(prices[i].Price - mean);
         }
         dev = (int)(0.5 + 100* (totDev / n) / price);




         int tmp = (int)(0.5 + 100 * (price - min) / (max - min));
         posLogger.Log("{0}: min={1}, max={2}, cnt={3}, lim={4}, ret={5}, dev={6}", name, min, max, prices.Count, n, tmp, dev); 

         return (int)(0.5 + 100 * (price - min) / (max - min));
      }

      private String saveInner(HtmlNode node)
      {
         return node == null ? "NULL" : node.InnerHtml;
      }
      private void loadHistory(List<PricePerDate> prices, HtmlDocument doc, String expr)
      {
         var nodes = doc.DocumentNode.SelectNodes(expr);
         if (nodes == null || nodes.Count != 1)
            throw new BMException("No nodes or more than 1 node found for expr \"{0}\".", expr);

         var rows = nodes[0].SelectNodes("tr");
         for (int i=1; i<rows.Count; i++)
            prices.Add (new PricePerDate(rows[i]));
      }

      private StringDict getAttributes(XmlNode node)
      {
         StringDict ret = new StringDict();
         var coll = node.Attributes;
         for (int i=0; i<coll.Count; i++)
         {
            var att = coll[i];
            if (att.LocalName.Equals ("url", StringComparison.InvariantCultureIgnoreCase)) continue;
            if (att.LocalName.Equals ("baseurl", StringComparison.InvariantCultureIgnoreCase)) continue;
            ret[att.LocalName] = att.Value;
         }
         return ret;
      }
      private void importUrl(PipelineContext ctx, IDatasourceSink sink, Regex regex, IDatasourceFeederElement elt)
      {
         StringDict attribs = getAttributes(elt.Context);
         Uri url = (Uri)elt.Element;
         const String expr = "//tr[@class='koersen_tabel_titelbalk']";
         sink.HandleValue (ctx, "html/_start", url);
         HtmlDocument doc = loadUrl(url);
         var nodes = doc.DocumentNode.SelectNodes(expr);
         if (nodes == null || nodes.Count != 1)
            throw new BMException("No nodes or more than 1 node found for expr \"{0}\".", expr);
         var tableNode = nodes[0].ParentNode;
         if (tableNode == null || (tableNode.Name != "tbody" && tableNode.Name != "table"))
            throw new BMException("Parent of {0} is a {1} instead of tbody/table.", expr, tableNode == null ? "null" : tableNode.Name);

         nodes = tableNode.SelectNodes("tr");
         for (int i = 1; i < nodes.Count; i++)
         {
            var tdNodes = nodes[i].SelectNodes("td");
            if (tdNodes == null || tdNodes.Count != 7)
               throw new BMException("Unexpected #td elements: {0}. Should be 7. Url={0}.", tdNodes == null ? 0 : tdNodes.Count, url);
            var anchorNode = tdNodes[0].SelectSingleNode("a");
            var href = anchorNode.GetAttributeValue("href", "");
            String code = null;
            var matches = regex.Matches (href);
            if (matches.Count > 0)
            {
               if (matches[0].Groups.Count > 1)
               {
                  code = matches[0].Groups[1].Value;
               }
            }
            if (code == null) throw new BMException("Cannot extract code from href={0}", href); 

            String name = anchorNode.InnerText;
            foreach (var kvp in attribs)
               sink.HandleValue(ctx, "html/record/" + kvp.Key, kvp.Value);

            sink.HandleValue(ctx, "html/record/name", name);
            sink.HandleValue(ctx, "html/record/code", code);
            sink.HandleValue(ctx, "html/record/price", toDouble(tdNodes[2].InnerText));
            sink.HandleValue(ctx, "html/record/priceOpened", toDouble(tdNodes[5].InnerText));
            sink.HandleValue(ctx, "html/record/date", date);

            posLogger.Log();

            if (needHistory)
            {
               List<PricePerDate> prices = loadHistory(url, code, name);
               int dev;
               sink.HandleValue(ctx, "html/record/history", toJArray(prices));
               sink.HandleValue(ctx, "html/record/pos3m", computePos(prices, date.AddMonths(-3), name, out dev));
               sink.HandleValue(ctx, "html/record/dev3m", dev);
               sink.HandleValue(ctx, "html/record/pos6m", computePos(prices, date.AddMonths(-6), name, out dev));
               sink.HandleValue(ctx, "html/record/dev6m", dev);
               sink.HandleValue(ctx, "html/record/pos12m", computePos(prices, date.AddMonths(-12), name, out dev));
               sink.HandleValue(ctx, "html/record/dev12m", dev);
               sink.HandleValue(ctx, "html/record/pos36m", computePos(prices, date.AddMonths(-36), name, out dev));
               sink.HandleValue(ctx, "html/record/dev36m", dev);
            }

            sink.HandleValue(ctx, "html/record", null);
         }
         sink.HandleValue (ctx, "html/_end", url);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         Regex regex = new Regex (@"instrumentcode=(.*)$", RegexOptions.CultureInvariant | RegexOptions.Compiled | RegexOptions.IgnoreCase);
         foreach (var elt in feeder)
         {
            try
            {
               importUrl(ctx, sink, regex, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, e.Message + "\r\nUrl=" + elt.Element + ".");
            }
         }
      }

   }

}
