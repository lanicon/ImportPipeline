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

namespace Tika
{
   public class ScriptExtensions
   {
      int xx;
      public Object ErrorOn2(PipelineContext ctx, String key, Object value)
      {
         if (++xx > 2) throw new Exception ("ErrorOn2");
         return null;
      }
      public Object OnAdd(PipelineContext ctx, String key, Object value)
      {
         var ep = ctx.Action.Endpoint;
         String type = ep.GetFieldAsStr("doctype");
         String title = ep.GetFieldAsStr("title");
         String subject = ep.GetFieldAsStr("subject");

         if (String.IsNullOrEmpty(title) && !String.IsNullOrEmpty (subject))
         {
            title = subject;
            ep.SetField ("title", subject);
         }

         setSortSubject (ep, subject, type);

         JToken lastMod = ep.GetFieldAsToken("date_modified");
         if (lastMod == null)
         {
            String otherField = ep.GetFieldAsStr("doctype") == "Mail" ? "date_created" : "filedate";
            lastMod = ep.GetFieldAsToken(otherField);
            if (lastMod != null)  ep.SetField ("date_modified", lastMod);
         }

         if (lastMod != null && ep.GetFieldAsToken("year_modified")==null)
            ep.SetField ("year_modified", ((DateTime)lastMod).ToLocalTime().Year);

         //ctx.ImportLog.Log ("ADD: " + ep.GetField(null));
         return value;
      }
       
      private static String [] SEPS = new String[] {": "};
      private static void setSortSubject(IDataEndpoint ep, String subject, String type)
      {
         if (subject == null || type != "Mail") return;
         String sortSubject = subject;
         String[] arr = subject.Split (SEPS, StringSplitOptions.None);
         if (arr.Length <= 1) goto EXIT_RTN;

         int i;
         for (i = 0; i < arr.Length - 1; i++)
         {
            String part = arr[i];
            if (part.Length == 0) break;
            if (!char.IsUpper(part[0])) break;
            if (!onlyAlpha(part)) break;
         }
         if (i == 0) goto EXIT_RTN;

         sortSubject = arr[i];
         String words = arr[0];
         for (int j=1; j<i; j++) words = words + " " + arr[j];
         for (int j=i+1; j<arr.Length; j++) sortSubject = sortSubject + ": " + arr[j];
         ep.SetField("sort_subject_prefixes", words);

      EXIT_RTN:
         if (String.IsNullOrEmpty(sortSubject))
            sortSubject = " ";
         ep.SetField("sort_subject", sortSubject);
      }

      private static bool onlyAlpha(String txt)
      {
         for (int i = 0; i < txt.Length; i++)
         {
            if (!char.IsLetter(txt[i])) return false;
         }
         return true;
      }
      
  }

}