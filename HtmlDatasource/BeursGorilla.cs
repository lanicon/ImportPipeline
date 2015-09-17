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

   public class BeursGorillaDS : Datasource, IDatasourceSink
   {
      private DateTime date;
      private IDatasourceFeeder feeder;
      private bool needHistory;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         date = DateTime.UtcNow;
         feeder = ctx.CreateFeeder (node);
         needHistory = node.ReadBool("@history", true);
      }


      private static double toDouble(String x)
      {
         return Invariant.ToDouble(x.Replace(',', '.'));
      }
      private static double toDouble(String x, double def)
      {
         return Invariant.ToDouble(x.Replace(',', '.'), def);
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
            Date = new DateTime(Invariant.ToInt32(arr[2]), Invariant.ToInt32(arr[1]), Invariant.ToInt32(arr[0]), 0, 0, 0, DateTimeKind.Utc);
            Price = toDouble(cols[1].InnerText);
         }
         public PricePerDate(DateTime date, double price)
         {
            Date = ToUtcDateWithoutTime (date);
            Price = price;
         }
         public JObject ToJObject()
         {
            var ret = new JObject();
            ret.WriteToken("price", Price);
            ret.WriteToken("date", Date);
            return ret;
         }

         public static DateTime ToUtcDateWithoutTime(DateTime x)
         {
            return new DateTime(x.Year, x.Month, x.Day, 0, 0, 0, DateTimeKind.Utc);
         }

      }
      private List<PricePerDate> loadHistory(Uri baseUri, String id, String name)
      {
         const String fmt = "/fonds-informatie.asp?naam={1}&cat=historie&symbol=&instrumentcode={0}&subcat=11";
         HtmlDocument doc = loadUrl(new Uri(baseUri, String.Format(fmt, id, name)));

         loadHistory(doc, "//table[@class='fonds_info_koersen_links']");
         loadHistory(doc, "//table[@class='fonds_info_koersen_rechts']");

         var prices = history.Prices;
         if (prices.Count < 100)
            Logs.ErrorLog.Log (_LogType.ltWarning, "Unexpected small history: {0} days. Name={1}, Id={2}", prices.Count, name, id);
         return prices;
      }
      private void loadHistory(HtmlDocument doc, String expr)
      {
         var nodes = doc.DocumentNode.SelectNodes(expr);
         if (nodes == null || nodes.Count != 1)
            throw new BMException("No nodes or more than 1 node found for expr \"{0}\".", expr);

         var rows = nodes[0].SelectNodes("tr");
         for (int i = 1; i < rows.Count; i++)
            history.Add(new PricePerDate(rows[i]));
      }

      private static JArray toJArray(List<PricePerDate> prices)
      {
         JArray ret = new JArray();
         for (int i = 0; i < prices.Count; i++)
            ret.Add(prices[i].ToJObject());
         return ret;
      }

      static Logger posLogger = Logs.CreateLogger ("pos", "pos");
      class Stats
      {
         public double priceLo;
         public double priceHi;
         public double price;
         public double stdDev;
         public int position;
         public int possibleGain;
         public int score;

         public override string ToString()
         {
            return String.Format("prices={0} [{1} - {2}] pos={3}, gain={4}, score={5}, std={6}", price, priceLo, priceHi, position, possibleGain, score, stdDev);
         }
      }
      private static int toPercentage(double x)
      {
         return (int)(0.5 + 100 * x);
      }
      private Stats computePos(List<PricePerDate> prices, DateTime minDate, String name)
      {
         Stats ret = new Stats();
         if (prices.Count < 1) return ret;
         var elt = prices[0];
         double min = elt.Price;
         double max = elt.Price;
         double price = elt.Price;
         double mean = price;
         double totDev = 0.0;
         ret.price = price;

         int n = 1;
         for (int i = 1; i < prices.Count; i++)
         {
            elt = prices[i];
            if (elt.Date < minDate) break;
            n++;
            if (elt.Price > max) max = elt.Price;
            else if (elt.Price < min) min = elt.Price;
         }

         ret.priceLo = min;
         ret.priceHi = max;

         mean = mean / n;
         for (int i = 0; i < n; i++)
         {
            totDev += Math.Abs(prices[i].Price - mean);
         }
         ret.stdDev = (int)(0.5 + 100* (totDev / (n)) / price);
         double pos =  (price - min) / (max - min);
         double gain = (max - price) / price;

         ret.score = toPercentage ((1.0-pos)*gain);
         ret.position = toPercentage(pos);
         ret.possibleGain = toPercentage(gain);
         posLogger.Log("{0}: {1}, N={2}/{3}", name, ret, n, prices.Count);

         return ret;
      }

      private String saveInner(HtmlNode node)
      {
         return node == null ? "NULL" : node.InnerHtml;
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

      private History history;
      private void importGorillaUrl(PipelineContext ctx, IDatasourceSink sink, Regex regex, IDatasourceFeederElement elt)
      {
         StringDict attribs = getAttributes(elt.Context);
         Uri url = (Uri)elt.Element;

         int offset = elt.Context.ReadInt("@offset", 0);
         ctx.DebugLog.Log("-- Node={0}", elt.Context.OuterXml);

         ctx.DebugLog.Log("-- Fetching url " + url + ", offset=" + offset);
         const String expr = "//tr[@class='koersen_tabel_fondsen']";
         sink.HandleValue(ctx, "_start", url);
         HtmlDocument doc = loadUrl(url);
         var nodes = doc.DocumentNode.SelectNodes(expr);
         ctx.DebugLog.Log("-- -- <tr> count: {0}", nodes.Count); //.koersen_tabel_fondsen
         if (nodes.Count == 0)
            throw new BMException("No nodes found for expr \"{0}\".", expr);

         for (int i = offset; i < nodes.Count; i++)
         {
            ctx.IncrementEmitted();
            var trNode = nodes[i];
            var tdNodes = trNode.SelectNodes("td");
            if (tdNodes == null || tdNodes.Count != 7)
            {
               ctx.DebugLog.Log("Unexpected <tr> node:");
               ctx.DebugLog.Log(trNode.OuterHtml);
               throw new BMException("Unexpected #td elements: {0}. Should be 7. Url={0}.", tdNodes == null ? 0 : tdNodes.Count, url);
            }
            var anchorNode = tdNodes[0].SelectSingleNode("a");
            var href = anchorNode.GetAttributeValue("href", "");
            String code = null;
            //code = trNode.GetAttributeValue("id", null);
            var matches = regex.Matches(href);
            if (matches.Count > 0)
            {
               if (matches[0].Groups.Count > 1)
               {
                  code = matches[0].Groups[1].Value;
               }
            }
            if (code == null)
            {
               ctx.DebugLog.Log(trNode.OuterHtml);
               throw new BMException("Cannot extract code from href={0}", href);
            }

            String name = anchorNode.InnerText;
            ctx.DebugLog.Log("-- -- STOCK[{0}]: {1}", i, name);
            foreach (var kvp in attribs)
               sink.HandleValue(ctx, "record/" + kvp.Key, kvp.Value);

            double price = toDouble(tdNodes[2].InnerText, -1);
            String pricePrev = tdNodes[5].InnerText.TrimToNull();
            double priceOpened = pricePrev == null ? price : toDouble(pricePrev);
            if (price < 0) price = priceOpened;
            
            sink.HandleValue(ctx, "record/name", name);
            sink.HandleValue(ctx, "record/code", code);
            sink.HandleValue(ctx, "record/price", price);
            sink.HandleValue(ctx, "record/priceOpened", priceOpened);

            String time = tdNodes[6].InnerText.TrimToNull();
            if (time != null && !time.Equals("details", StringComparison.InvariantCultureIgnoreCase))
               sink.HandleValue(ctx, "record/checktime", time);
            sink.HandleValue(ctx, "record/date", date);

            posLogger.Log();

            if (needHistory)
            {
               history = new History();
               histDate = DateTime.MinValue;
               histPrice = double.MinValue;

               history.Add(date, price);
               sink.HandleValue(ctx, "record/_preparehistory", null);
               List<PricePerDate> prices = loadHistory(url, code, name);
               if (prices.Count <= 1)
               {
                  ctx.ErrorLog.Log("Share without history detected: " + name);
                  this.SharesWithoutHistory++;
               }

               Stats stats3m = computePos(prices, date.AddMonths(-3), name);
               Stats stats6m = computePos(prices, date.AddMonths(-6), name);
               Stats stats12m = computePos(prices, date.AddMonths(-12), name);
               Stats stats36m = computePos(prices, date.AddMonths(-36), name);

               sink.HandleValue(ctx, "record/pos3m", stats3m.position);
               sink.HandleValue(ctx, "record/pos6m", stats6m.position);
               sink.HandleValue(ctx, "record/pos12m", stats12m.position);
               sink.HandleValue(ctx, "record/pos36m", stats36m.position);

               sink.HandleValue(ctx, "record/gain3m", stats3m.possibleGain);
               sink.HandleValue(ctx, "record/gain6m", stats6m.possibleGain);
               sink.HandleValue(ctx, "record/gain12m", stats12m.possibleGain);
               sink.HandleValue(ctx, "record/gain36m", stats36m.possibleGain);

               sink.HandleValue(ctx, "record/score3m", stats3m.score);
               sink.HandleValue(ctx, "record/score6m", stats6m.score);
               sink.HandleValue(ctx, "record/score12m", stats12m.score);
               sink.HandleValue(ctx, "record/score36m", stats36m.score);
            
               sink.HandleValue(ctx, "record/stddev3m", stats3m.stdDev);
               sink.HandleValue(ctx, "record/stddev6m", stats6m.stdDev);
               sink.HandleValue(ctx, "record/stddev12m", stats12m.stdDev);
               sink.HandleValue(ctx, "record/stddev36m", stats36m.stdDev);

               sink.HandleValue(ctx, "record/history", toJArray(prices));
            }

            sink.HandleValue(ctx, "record", null);
         }
         sink.HandleValue(ctx, "_end", url);
      }

      private int SharesWithoutHistory; 
      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         SharesWithoutHistory = 0;
         Regex regex = new Regex (@"instrumentcode=(.*)$", RegexOptions.CultureInvariant | RegexOptions.Compiled | RegexOptions.IgnoreCase);
         foreach (var elt in feeder.GetElements(ctx))
         {
            try
            {
               importGorillaUrl(ctx, sink, regex, elt);
            }
            catch (Exception e)
            {
               throw new BMException(e, e.Message + "\r\nUrl=" + elt.Element + ".");
            }
         }
         if (SharesWithoutHistory > 0)
         {
            ctx.ErrorLog.Log("{0} Shares without history detected", SharesWithoutHistory);
            //throw new BMException("{0} Shares without history detected", SharesWithoutHistory);
         }
      }

      private DateTime histDate;
      private double histPrice;
      public object HandleValue(PipelineContext ctx, string key, object value)
      {
         if (history != null)
         {
            if ((ctx.ImportFlags & _ImportFlags.TraceValues) != 0) ctx.DebugLog.Log("HIST HandleValue ({0}, {1} [{2}]", key, value, value == null ? "null" : value.GetType().Name);
            switch (key.ToLowerInvariant())
            {
               case "history/_v/date": histDate = (DateTime)value; break;
               case "history/_v/price": histPrice = (double)value; break;
               case "history/_v":
                  if (histDate == DateTime.MinValue || histPrice < 0)
                     throw new BMException("Unexpected history: date={0}, price={1}", histDate, histPrice);
                  history.Add(histDate, histPrice);
                  histDate = DateTime.MinValue;
                  histPrice = double.MinValue;
                  break;
            }
         }
         return null;
      }

      public bool HandleException(PipelineContext ctx, string prefix, Exception err)
      {
         return false;
      }

      class History
      {
         private List<PricePerDate> prices;
         private Dictionary<DateTime, PricePerDate> dict;
         private int maxCount;
         public List<PricePerDate> Prices { get { return getPrices(); } }

         private List<PricePerDate> getPrices()
         {
            prices.Sort((a, b) => Comparer<DateTime>.Default.Compare(b.Date, a.Date));
            if (maxCount >= 0 && maxCount < prices.Count)
            {
               prices.RemoveRange(maxCount, prices.Count - maxCount); 
            }
            return prices;
         }

         public History(int maxCount=-1)
         {
            this.maxCount = maxCount;
            prices = new List<PricePerDate>();
            dict = new Dictionary<DateTime, PricePerDate>();
         }

         public void Add(DateTime date, double price)
         {
            if (dict.ContainsKey(date)) return;
            var ppd = new PricePerDate(date, price);
            prices.Add(ppd);
            dict.Add(ppd.Date, ppd);
         }
         public void Add(PricePerDate ppd)
         {
            if (dict.ContainsKey(ppd.Date)) return;
            prices.Add(ppd);
            dict.Add(ppd.Date, ppd);
         }
      }
   }



}
