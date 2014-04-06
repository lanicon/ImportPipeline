//@ref=bmjson100.dll
//@ref=bmelastic100.dll
//@ref=Newtonsoft.Json.dll
using Bitmanager.Core;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using Bitmanager.ImportPipeline;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;

namespace Giro {
   class RecordChecker
   {
      ESDataEndpoint ep;
      StringDict<bool> presentKeys;
      PipelineContext ctx;
      public RecordChecker(PipelineContext ctx, IDataEndpoint endpoint)
      {
         this.ctx = ctx;
         ctx.ImportLog.Log("Create RecordChecker with {0}", endpoint);
         presentKeys = new StringDict<bool>();
         ep = endpoint as ESDataEndpoint;
         if (ep == null) return;

         var e = ep.Connection.CreateEnumerator (ep.DocType.UrlPart);
         foreach (var rec in e)
         {
            var date = rec.GetField("date");
            if (String.IsNullOrEmpty(date)) continue;
            presentKeys[date] = true;
         }
         ctx.ImportLog.Log("-- loaded {0} keys", presentKeys.Count);
      }
      public bool IsPresent(String key)
      {
         int entries = presentKeys.Count; 
         bool ret = _IsPresent(key);
         ctx.ImportLog.Log("IsPresent ({0})-->{1} (entries={2})", key, ret, entries);
         return ret;
      }
      public bool IsPresent(String key, RecordChecker main)
      {
         int entries = presentKeys.Count;
         bool ret = _IsPresent(key, main);
         ctx.ImportLog.Log("IsPresent ({0}, {3})-->{1}  (entries={2})", key, ret, entries, main);
         return ret;
      }

      public bool _IsPresent(String key)
      {
         bool ret;
         if (presentKeys.TryGetValue(key, out ret)) return ret;
         presentKeys.Add(key, true);
         return false;
      }
      public bool _IsPresent(String key, RecordChecker main)
      {
         bool ret;
         if (presentKeys.TryGetValue(key, out ret)) return ret;
         ret = main.IsPresent(key);
         presentKeys.Add(key, ret);
         return ret;
      }
   }
   public class ScriptExtension
   {
      RecordChecker indexChecker;
      RecordChecker fileChecker;
      public ScriptExtension()
      {
         Logs.ErrorLog.Log("__CREATED__");
      }

      String name = null;
      public Object OnDSStart(PipelineContext ctx, String key, Object value)
      {
         if (indexChecker == null) indexChecker = new RecordChecker(ctx, ctx.Action.Endpoint);
         fileChecker = new RecordChecker(ctx, null);
         return value;
      }
      public Object OnAdd(PipelineContext ctx, String key, Object value)
      {
         //ctx.ImportLog.Log("accu={0}", ((JObject)ctx.Action.Endpoint.GetFieldAsToken(null)).Count);
         if (fileChecker.IsPresent(ctx.Action.Endpoint.GetFieldAsStr("date"), indexChecker))
         {
            ctx.ClearAllAndSetFlags();
            return value;
         }

         SortAndUndupField(ctx.Action.Endpoint, "account_other", compare);
         CalculateFacetValues (ctx.Action.Endpoint);
         return value;
      }
      
      private int compare (String a, String b)
      {
         int rc = b.Length - a.Length;
         if (rc != 0) return rc;
         return String.Compare (a, b, StringComparison.InvariantCultureIgnoreCase);
      }
      
      
      private object SortAndUndupField (IDataEndpoint endpoint, String fld, Comparison<String> comparer)
      {
         Object val = endpoint.GetFieldAsToken ("account_other");
         if (UndupAndSort (comparer, ref val))
            endpoint.SetField (fld, val, FieldFlags.OverWrite, null);
         return val;
      } 

      private void CalculateFacetValues (IDataEndpoint endpoint)
      {
         int date = endpoint.GetFieldAsInt32("date");
         endpoint.SetField ("year", date / 10000); 
         endpoint.SetField ("month", (date / 100) % 100); 
         endpoint.SetField ("day", date % 100); 

         double amount = endpoint.GetFieldAsDbl ("amount");
         String type = endpoint.GetFieldAsStr("type").ToLowerInvariant();
         if (type == "af")
         {
            endpoint.SetField("amount_neg", amount);
            endpoint.SetField("amount_tot", -amount);
         }
         else
         {
            endpoint.SetField("amount_pos", amount);
            endpoint.SetField("amount_tot", amount);
         }
         String nameFacet = endpoint.GetFieldAsStr("name");
         String mut = endpoint.GetFieldAsStr("mutation_code").ToLowerInvariant();
         switch (mut)
         {
            default:
               nameFacet = endpoint.GetFieldAsStr("name"); break;
            case "ba":
               nameFacet = endpoint.GetFieldAsStr("comment"); 
               int ix = nameFacet.IndexOf(',');
               if (ix >= 0) nameFacet = nameFacet.Substring(0, ix);
               break;
         }
         endpoint.SetField("name_facet", nameFacet);

         var accountOther = endpoint.GetFieldAsToken("account_other");
         if (accountOther != null)
         {
            var accountFacets = new List<String>();
            foreach (var jt in accountOther)
            {
               String acc = (String)jt;
               if (acc.IndexOf(' ') >= 0) continue;
               accountFacets.Add(acc);
            }
            if (accountFacets.Count > 0) endpoint.SetField("account_other_facet", accountFacets);
            //account_other_facet
         }
      }

      public String ToLowerIfOnlyUpper(String x)
      {
         if (String.IsNullOrEmpty(x)) return x;
         int lc = 0;
         int uc = 0;
         for (int i = 0; i < x.Length; i++)
         {
            if (Char.IsLower(x[i])) { ++lc; continue; }
            if (Char.IsUpper(x[i])) { ++uc; continue; }
         }
         return (lc == 0 && uc > 0) ? x.ToLowerInvariant() : x;
      }
      public bool UndupAndSort(Comparison<String> comparison, ref Object obj)
      {
         if (obj==null) return false;
         Logs.DebugLog.Log ("Undup: arr={0}", obj.GetType().FullName);
         var arr = obj as JArray;
         if (arr==null || arr.Count <= 1) return false;

         List<String> list = new List<String>(arr.Count);
         for (int i = 0; i < arr.Count; i++)
         {
            String x = (String)arr[i];
            if (x==null) continue;
            list.Add (x);
         }
         list.Sort(comparison);
         for (int i = list.Count-1; i>0; i--)
         {
            String a = list[i-1];
            if (String.Equals (a, list[i], StringComparison.InvariantCultureIgnoreCase))
               list.RemoveAt(i); 
         } 
         obj = list;
         return true;
      }
      public Object SaveName (PipelineContext ctx, String key, Object value)
      {
         name = (value == null) ? null : value.ToString();
         return value;
      }

      private StringBuilder prepareValue(Object value)
      {
         StringBuilder ret = new StringBuilder();
         String tmp = value.ToString();
         int state = 0;
         for (int i = 0; i < tmp.Length; i++)
         {
            if (!Char.IsWhiteSpace(tmp[i])) 
            {
               switch (state)
               {
                  case 2: ret.Append(' '); break;
                  case 3: ret.Append(", "); break;
               }
               state = 1; 
               ret.Append(tmp[i]); continue;
            }
            switch (state)
            {
               case 0: continue;
               case 1: state = 2;  continue;
               case 2: state = 3;  continue;
               case 3: continue;
            }
         }
         ret.Append(" A: B");  
         return ret;
      }

      public Object OnDescription(PipelineContext ctx, String key, Object value)
      {
         if (value == null) return null;
         StringBuilder desc = prepareValue(value);
         List<Part> parts = new List<Part>();

         Part lastPart = null;
         Part cur = new Part();
         for (int i=0; i<desc.Length; i++)
         {
            if (desc[i] != ':') continue;
            ctx.DebugLog.Log("possible part [{0}/{1}]: '{2}'", i, desc.Length, desc.ToString(i, desc.Length-i));
            if (i==desc.Length-1) continue; // || desc[i+1] != ' ') continue;
            if (i==0 || !char.IsLetter (desc[i-1])) continue;
            cur.SetKey(desc, i); 

            if (lastPart != null)
            {
               lastPart.SetEndIndex(desc, cur.keyStart);
               parts.Add(lastPart);
            }
            else
            {
               if (cur.keyLen > 0)
               {
                  lastPart = new Part();
                  lastPart.SetEndIndex(desc, cur.keyStart); 
                  parts.Add(lastPart);
               }
            }
            lastPart = cur;
            cur = new Part();
         }
         
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < parts.Count; i++)
         {
            ctx.DebugLog.Log("[{0}]: {1}", i, parts[i]);
         }
         for (int i = 0; i < parts.Count; i++)
         {
            Part x = parts[i];
            String descKey = x.GetKey(desc).ToEmptyIfNull();
            switch (descKey.ToLowerInvariant())
            {
               default: break;

               case "naam":
                  if (String.Equals (name, x.GetVal(desc), StringComparison.InvariantCultureIgnoreCase)) continue; 
                  break;

               case "iban":
                  var iban = new IBAN(x.GetVal(desc));
                  ctx.Pipeline.HandleValue (ctx, "record/ibanfromdesc", iban.FormattedIban);
                  ctx.Pipeline.HandleValue (ctx, "record/ibanfromdesc", iban.ToString());
                  //ctx.Pipeline.HandleValue (ctx, "record/ibanfromdesc", iban.Account); //Zit al in formatted...
                  ctx.Pipeline.HandleValue (ctx, "record/ibanfromdesc", iban.TrimmedAccount);
                  continue;
            }
            sb.AppendIfNotNullOrEmpty (x.GetAll (desc), ", ");
         }
         //return ToLowerIfOnlyUpper(sb.ToString());
         return sb.ToString();
      }
   }
   
   class Part
   {
      public int keyStart;
      public int keyLen;
      public int valStart;
      public int valLen;

      public String GetKey(StringBuilder container)
      {
         if (keyLen <= 0) return null;
         return container.ToString(keyStart, keyLen);
      }
      public String GetVal(StringBuilder container)
      {
         if (valLen <= 0) return null;
         return container.ToString(valStart, valLen);
      }

      public String GetAll(StringBuilder container)
      {
         int len = valStart + valLen - keyStart;
         return container.ToString(keyStart, len);
      }

      public void SetKey(StringBuilder sb, int index)
      {
         int end = -1;
         int start = -1;
         int state = 0;
         for (int i = index - 1; i >= 0; i--)
         {
            char ch = sb[i];
            if (Char.IsWhiteSpace(ch))
            {
               if (state == 0) continue;
               start = i + 1;
               break;
            }
            if (!char.IsLetterOrDigit(ch))
            {
               start = i + 1;
               break;
            }

            if (char.IsDigit(ch))
            {
               switch (state)
               {
                  case 0: end = i+1; state = 1; continue;
                  case 3: start = i + 1; goto END;
               }
               start = i;
               continue;
            }

            if (char.IsLower(ch))
            {
               switch (state)
               {
                  case 0: end = i+1; state = 2; continue;
                  case 3: start = i + 1; goto END;
               }
               start = i;
               continue;
            }

            //Uppercase
            switch (state)
            {
               case 0: end = i+1; state = 3; continue;
               case 1:
               case 2:
                  start = i;
                  goto END;
               case 3:
                  start = i;
                  continue;
            }
            //start = i + 1;
            //break;
         }
      END:
         if (start < 0 || end < 0)
         {
            keyStart = index;
            keyLen = 1;
            return;
         }
         keyStart = start;
         keyLen = end - start;
      }


      internal void SetEndIndex(StringBuilder desc, int index)
      {
         int end;
         for (end = index - 1; end >= 0 && (char.IsWhiteSpace(desc[end]) || desc[end] == ':'); end--) ;
         end++;

         int start;
         for (start = keyStart+keyLen; start < end && (char.IsWhiteSpace(desc[start]) || desc[start] == ':'); start++) ;

         valStart = start;
         valLen = end - start;
      }

      public override string ToString()
      {
         return String.Format ("Part [start={0}, end={1}, keylen={2}, valLen={3}]", keyStart, valStart+valLen, keyLen, valLen);
      }
   }

   public class IBAN
   {
      public readonly String Country;
      public readonly String Bank;
      public readonly String Account;
      public readonly String TrimmedAccount;
      public readonly String FormattedIban;
      public readonly String Iban;

      private static readonly char[] TRIM_CHARS = { '0' };
      public IBAN(String iban)
      {
         String tmp = iban.Replace(" ", "");
         int accnr = tmp.Length - 1;
         for (; accnr >= 0; accnr--)
         {
            if (tmp[accnr] < '0' || tmp[accnr] > '9') break;
         }
         accnr++; // gives start of the account nr

         int state = 0;
         int bank = 0;

         for (int i = 0; i < accnr; i++)
         {
            if (tmp[i] < '0' || tmp[i] > '9')
            {
               if (state == 0) continue;
               bank = i;
               break;
            }
            state = 1;
         }
         Country = tmp.Substring(0, bank).ToNullIfEmpty();
         Bank = tmp.Substring(bank, accnr - bank).ToNullIfEmpty();
         Account = tmp.Substring(accnr);
         if (Account != null) TrimmedAccount = Account.TrimStart(TRIM_CHARS);
         StringBuilder sb = new StringBuilder();
         sb.Append (Country);
         sb.AppendIfNotNull (Bank, ' ').AppendIfNotNull (Account, ' ');
         FormattedIban = sb.ToString();
         Iban = FormattedIban.Replace(" ", "");
      }

      public override string ToString()
      {
         return Iban;
      }
   }
}