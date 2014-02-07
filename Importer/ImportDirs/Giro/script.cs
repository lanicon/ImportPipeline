//@ref=bmjson100.dll
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

   public class ScriptExtension
   {
      String name = null;
      public Object OnAdd (PipelineContext ctx, String key, Object value)
      {
         SortAndUndupField (ctx.Action.EndPoint, "account_other", compare);
         return value;
      }
      
      private int compare (String a, String b)
      {
         int rc = b.Length - a.Length;
         if (rc != 0) return rc;
         return String.Compare (a, b, StringComparison.InvariantCultureIgnoreCase);
      }
      
      
      private void SortAndUndupField (IDataEndpoint endpoint, String fld, Comparison<String> comparer)
      {
         Object val = endpoint.GetField ("account_other");
         if (UndupAndSort (comparer, ref val))
            endpoint.SetField (fld, val, FieldFlags.OverWrite, null);
      } 
      
      public bool UndupAndSort (Comparison<String> comparison, ref Object obj)
      {
         JArray arr = obj as JArray;
         if (arr==null || arr.Count <= 1) return false;
         
         List<String> list = new List<String> (arr.Count); 
         for (int i=0; i<arr.Count; i++)
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
         obj = new JArray (list);
         return true;
      }
      public Object SaveName (PipelineContext ctx, String key, Object value)
      {
         name = (value == null) ? null : value.ToString();
         return value;
      }
      public Object OnDescription (PipelineContext ctx, String key, Object value)
      {
         if (value == null) return null;
         String desc = value.ToString()+" A: b";
         List<Part> parts = new List<Part>();

         Part lastPart = null;
         Part cur = new Part();
         for (int i=0; i<desc.Length; i++)
         {
            if (desc[i] != ':') continue;
            if (i==desc.Length-1 || desc[i+1] != ' ') continue;
            if (i==0 || !char.IsLetter (desc[i-1])) continue;

            cur.dotIndex = i;
            int j=i-1;
            bool upperFound = false;
            for (; j>=0; j--)
            {
               if (!char.IsLetter(desc[j])) break;
               if (char.IsLower(desc[j]))
               {
                  if (upperFound) break;
                  continue;
               }
               upperFound = true;
            }
            j++;
            cur.startIndex = j;
            if (lastPart != null)
            {
               lastPart.endIndex = j;
               parts.Add(lastPart);
            }
            else
            {
               if (j > 0)
               {
                  lastPart = new Part();
                  lastPart.startIndex = 0;
                  lastPart.endIndex = j;
                  lastPart.dotIndex = -1;
                  parts.Add(lastPart);
               }
            }
            lastPart = cur;
            cur = new Part();
         }
         
         StringBuilder sb = new StringBuilder();
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
                  ctx.Pipeline.HandleValue (ctx, "record/ibanfromdesc", iban.Account);
                  ctx.Pipeline.HandleValue (ctx, "record/ibanfromdesc", iban.TrimmedAccount);
                  break;
            }
            sb.AppendIfNotNullOrEmpty (x.GetAll (desc), ", ");
         } 
         return sb.ToString();
      }
   }
   
   class Part
   {
      public int startIndex;
      public int dotIndex;
      public int endIndex;

      public String GetKey(String container)
      {
         if (dotIndex < 0) return null;
         return container.Substring(startIndex, dotIndex - startIndex);
      }
      public String GetVal(String container)
      {
         if (endIndex < startIndex) return "??";

         if (dotIndex < 0) return container.Substring(startIndex, endIndex - startIndex);

         return container.Substring(dotIndex+2, endIndex - dotIndex- 2);
      }
      
      public String GetAll (String container)
      {
         return container.Substring(startIndex, endIndex - startIndex);
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
         sb.AppendIfNotNull (Country).AppendIfNotNull (Bank, ' ').AppendIfNotNull (Account, ' ');
         FormattedIban = sb.ToString();
         Iban = FormattedIban.Replace(" ", "");
      }

      public override string ToString()
      {
         return Iban;
      }
   }

   
