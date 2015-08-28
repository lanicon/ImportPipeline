using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Net.Mail;
using System.IO;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// This class is responsible for sending a report about he import  
   /// It is configured from the import.xml by the node "report"
   /// </summary>
   public class MailReporter
   {
      public enum _Mode { Always, Never, Errors}

      public readonly MailAddress[] MailTo;
      public readonly MailAddress MailFrom;
      public readonly String MailSubject;
      public readonly String MailServer;
      public readonly int Port;
      public readonly _Mode Mode;
      public MailReporter(XmlNode node)
      {
         String[] to = node.ReadStr("@to").Split(';');

         MailTo = new MailAddress[to.Length];
         for (int i = 0; i < to.Length; i++)
            MailTo[i] = new MailAddress(to[i]);

         String from = node.ReadStr("@from", null);
         MailFrom = from==null ? new MailAddress(MailTo[0].Address, "Noreply") : new MailAddress(from);

         MailSubject = node.ReadStr("@subject", "[Import {2}] for {1}");
         MailServer = node.ReadStr("@server");
         int ix = MailServer.IndexOf(':');
         if (ix < 0)
            Port = 25;
         else
         {
            Port = Invariant.ToInt32(MailServer.Substring(ix + 1));
            MailServer = MailServer.Substring(0, ix);
         }
         Mode = node.ReadEnum("@mode", _Mode.Errors);
      }

      public void SendReport (PipelineContext ctx, ImportReport report)
      {
         if ((ctx.ImportEngine.ImportFlags & _ImportFlags.NoMailReport) != 0)
            return;

         switch (Mode)
         {
            case _Mode.Never: return;
            case _Mode.Always: break;
            case _Mode.Errors:
               if (!report.HasErrors) return;
               break;
            default:
               Mode.ThrowUnexpected();
               break;
         }

         StringBuilder sb = new StringBuilder();
         String fn = ctx.ImportEngine.Xml.FileName;
         sb.AppendFormat("Import report for {0}.\r\n", fn);
         if (report.ErrorMessage != null)
            sb.AppendFormat("-- LastError={0}\r\n", indentLines (report.ErrorMessage, 6, false));
         sb.AppendFormat("-- Active datasources ({0}):\r\n", report.DatasourceReports.Count);
         for (int i = 0; i < report.DatasourceReports.Count; i++)
         {
            var dsrep = report.DatasourceReports[i];
            sb.AppendFormat("-- -- [{0}]: {1} \r\n", dsrep.DatasourceName, indentLines (dsrep.Stats, 9, false));
         }

         String shortName = Path.GetFileName(Path.GetDirectoryName(fn));
         ctx.ImportLog.Log ("Sending report to {0} (first address)...", MailTo[0]);
         using (SmtpClientEx smtp = new SmtpClientEx(MailServer, Port))
         {
            using (MailMessage m = new MailMessage())
            {
               m.From = MailFrom;
               foreach (var x in MailTo) m.To.Add(x);


               m.Subject = String.Format (MailSubject, fn, shortName, report.HasErrors ? "Errors" : "Report");
               m.SubjectEncoding = Encoding.UTF8;
               m.BodyEncoding = Encoding.UTF8;
               m.Body = sb.ToString();
               smtp.Send(m);
            }
         }
         ctx.ImportLog.Log("Report is sent successfull.");

      }
      private static String indentLines (String lines, int numIndent, bool firstToo)
      {
         if (lines == null) return null;
         String indent = new String(' ', numIndent);
         String[] arr = lines.Split('\n');
         StringBuilder sb = new StringBuilder(lines.Length + 256);
         foreach (String s in arr)
         {
            if (sb.Length > 0 || firstToo)
               sb.Append(indent);
            sb.AppendLine(s.TrimWhiteSpace());
         }
         return sb.ToString();
      }


      public class SmtpClientEx : SmtpClient
      {
         public SmtpClientEx(string host, int port)
            : base(host, port)
         {
         }

         public new void Send(MailMessage message)
         {
            try
            {
               base.Send(message);
            }
            catch (Exception e)
            {
               throw new BMException(e, "{0}. \r\nFrom={1}, To={2}.", e.Message, getMailAddr(message.From), getMailAddr(message.To));
            }
         }

         private String getMailAddr(MailAddress addr)
         {
            if (addr == null) return "NULL";
            return addr.Address;
         }
         private String getMailAddr(MailAddressCollection coll)
         {
            if (coll == null || coll.Count == 0) return "NULL";
            return getMailAddr(coll[0]);
         }
      }

   }
}
