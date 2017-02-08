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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using Bitmanager.Core;
using Bitmanager.Json;
using Bitmanager.IO;
using Bitmanager.Xml;
using Bitmanager.ImportPipeline.StreamProviders;
using Bitmanager.Elastic;
using Microsoft.Office.Interop.Excel;
using System.Text.RegularExpressions;
using Range = Microsoft.Office.Interop.Excel.Range;

namespace Bitmanager.ImportPipeline
{
   public class ExcelDatasource : StreamDatasourceBase
   {
      String prefix;
      String selectedSheets;
      Regex selectedSheetsExpr;
      List<String> eventKeys;
      int startAt, headersAt;
      public ExcelDatasource() : base(true, false) { }

      private List<String> prepareEventKeys(String name, int cnt, List<String> headers)
      {
         if (eventKeys == null)
            eventKeys = new List<string>(cnt + 1);
         else
            if (eventKeys.Count > 0 && eventKeys[0] != name)
               eventKeys.Clear();

         String pfx = prefix != null ? prefix : name;
         pfx = pfx.ToLowerInvariant();
         if (eventKeys.Count == 0)
            eventKeys.Add(pfx);
         pfx += '/'; 
         String pfx_f = pfx+'f'; 
         for (int i = eventKeys.Count; i <= cnt; i++)
         {
            String h = i<headers.Count ? headers[i] : null;
            if (h!=null) h = h.ToLowerInvariant().TrimToNull();
            String key = h==null ? pfx_f + Invariant.ToString (i-1) : pfx + h;
            eventKeys.Add (key);
         }
         return eventKeys;
      }
      public override void Init(PipelineContext ctx, XmlNode node, Encoding defEncoding)
      {
         base.Init(ctx, node, defEncoding);
         startAt = node.ReadInt("@startat", 0);
         headersAt = node.ReadInt("@headersat", -1);
         if (headersAt > startAt)
            throw new BMNodeException(node, "HeadersAt ({0}) should be < startat ({1}).", headersAt, startAt);
         prefix = node.ReadStr("@prefix", null);
         selectedSheets = node.ReadStr("@sheets", null);
         if (selectedSheets != null)
            selectedSheetsExpr = new Regex(selectedSheets, RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
      }

      protected void closeWorkbook (ref Workbook x)
      {
         Workbook tmp = x;
         x = null;
         if (tmp == null) return;
         tmp.Close();
         Utils.Free(tmp);
      }
      protected override void ImportStream(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Stream strm)
      {
         Workbook wb = null;
         var excel = new Microsoft.Office.Interop.Excel.Application();
         try
         {
            wb = excel.Workbooks.Open(elt.FullName);
            foreach (Microsoft.Office.Interop.Excel.Worksheet sheet in wb.Worksheets)
            {
               String name = sheet.Name;
               sink.HandleValue(ctx, "_sheet/_start", name);
               if (selectedSheetsExpr == null || selectedSheetsExpr.IsMatch(name))
                  importSheet(ctx, sink, elt, sheet);
               sink.HandleValue(ctx, "_sheet/_stop", name);
            }
         }
         finally
         {
            closeWorkbook(ref wb);
            Utils.FreeAndNil(ref excel);
         }
      }

      private static String _toString (Object o)
      {
         if (o == null) return null;

         StringBuilder sb = new StringBuilder();
         bool needSep = false;
         foreach (var c in o.ToString())
         {
            if (char.IsLetterOrDigit(c))
            {
               if (needSep) sb.Append('_');
               needSep = false;
               sb.Append(c);
               continue;
            }

            if (sb.Length > 0) needSep = true;
         }
         return sb.ToString();
      }
      private void importSheet(PipelineContext ctx, IDatasourceSink sink, IStreamProvider elt, Worksheet sheet)
      {
         Range used = sheet.UsedRange;
         Range usedCells = used.Cells;
         if (usedCells == null) return;

         Object[,] c = (Object[,])used.Cells.Value2;
         if (c == null) return;

         int lo1 = c.GetLowerBound(0);
         int hi1 = c.GetUpperBound(0);
         int lo2 = c.GetLowerBound(1);
         int hi2 = c.GetUpperBound(1);

         List<String> headers = new List<string>();
         if (headersAt >= 0)
         {
            int headersRow = lo1 + headersAt;
            if (headersRow <= hi1)
            {
               int h = 0;
               for (int j = lo2; j <= hi2; j++)
               {
                  for (; h < j; h++) headers.Add(null);
                  headers.Add(_toString (c[headersRow, j]));
                  h++;
               }
            }
         }

         var keys = prepareEventKeys(sheet.Name, hi2 + 1, headers);
         for (int i = lo1 + startAt; i <= hi1; i++)
         {
            for (int j = lo2; j <= hi2; j++)
               sink.HandleValue (ctx, keys[j], c[i, j]);
            sink.HandleValue(ctx, keys[0], null);
            ctx.IncrementEmitted();
         }
      }

      protected override Stream _CreateStream(IStreamProvider elt)
      {
         return null;
      }


   }
}
