using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public enum CsvTrimOptions
   {
      False = 0,
      None = 0,
      TrimUnquoted = 1,
      TrimQuoted = 2,
      All = 3,
      True = 3
   }

   public class CsvReader
   {
      private String fileName;
      private StreamReader reader;
      public bool SkipEmptyRecords;
      public bool SkipHeader;
      public CsvTrimOptions TrimOptions;
      private int line;
      private int nextChar;
      private int quoteChar;
      private int sepChar;
      private int escapeChar;
      private List<String> fields;
      public List<String> Fields { get { return fields; } }
      public int Line { get { return line; } }

      public String SepChar
      {
         get { return new String((char)sepChar, 1); }
         set { sepChar = String.IsNullOrEmpty(value) ? -1 : (int)value[0]; }
      }
      public String QuoteChar
      {
         get { return quoteChar < 0 ? null : new String((char)quoteChar, 1); }
         set { quoteChar = String.IsNullOrEmpty(value) ? -1 : (int)value[0]; }
      }
      public String EscapeChar
      {
         get { return escapeChar < 0 ? null : new String((char)escapeChar, 1); }
         set { escapeChar = String.IsNullOrEmpty(value) ? -1 : (int)value[0]; }
      }

      public int SepOrd
      {
         get { return sepChar; }
         set { sepChar = value; }
      }
      public int QuoteOrd
      {
         get { return quoteChar; }
         set { quoteChar = value; }
      }
      public int EscapeOrd
      {
         get { return escapeChar; }
         set { escapeChar = value; }
      }

      //public _Reader (using (StreamReader sr = new StreamReader("TestFile.txt")) 
      public CsvReader(StreamReader sr, String fileName=null)
      {
         TrimOptions = CsvTrimOptions.None;
         reader = sr;
         nextChar = sr.Read();
         fields = new List<string>();
         quoteChar = '"';
         sepChar = 9;
         escapeChar = -1;
         line = -1;
         SkipEmptyRecords = true;
         this.fileName = fileName;
      }
      public CsvReader(Stream strm, String fileName=null)
         : this(new StreamReader(strm, Encoding.UTF8, true, 4096), fileName)
      {
      }

      public bool NextRecord()
      {
         bool trimUnquoted = (TrimOptions & CsvTrimOptions.TrimUnquoted) != 0;
         bool trimQuoted = (TrimOptions & CsvTrimOptions.TrimQuoted) != 0;
         bool inQuotedField;

         while (true)  //Loop for processing multiple records (in case of the 1st header or skipping empty records)
         {
            inQuotedField = false;
            fields.Clear();
            if (nextChar < 0) return false;
            ++line;
            int ch = nextChar;

            StringBuilder sb = new StringBuilder();
            //int pos = 0;

            while (true)
            {
               switch (ch)
               {
                  case -1:
                     if (inQuotedField) throwError("EOF reached before end of a quoted field.");
                     if (fields.Count > 0 || sb.Length > 0) addFieldAndClear(sb, trimUnquoted);
                     goto EOR;

                  case '\r':
                  case '\n':
                     if (inQuotedField)
                     {
                        sb.Append((char)ch);
                        goto NEXT_CHAR;
                     }
                     if (fields.Count > 0 || sb.Length > 0) addFieldAndClear(sb, trimUnquoted);
                     if (ch == '\r')
                     {
                        ch = reader.Read();
                        if (ch != '\n') goto EOR;
                     }
                     else
                     {
                        ch = reader.Read();
                        if (ch != '\r') goto EOR;
                     }
                     ch = reader.Read();
                     goto EOR;
               }

               if (ch == sepChar)
               {
                  if (inQuotedField)
                  {
                     sb.Append((char)ch);
                     goto NEXT_CHAR;
                  }
                  addFieldAndClear(sb, trimUnquoted);
                  inQuotedField = false;
                  goto NEXT_CHAR;
               }

               if (ch == quoteChar)
               {
                  if (inQuotedField)
                  {
                     ch = reader.Read();
                     if (ch == quoteChar)
                     {
                        sb.Append((char)ch);
                        goto NEXT_CHAR;
                     }

                     while (ch == ' ') ch = reader.Read();
                     switch (ch)
                     {
                        default:
                           if (ch == sepChar) break;
                           throwInvalidChar(ch);
                           break;
                        case '\r':
                        case '\n':
                        case -1:
                           break;
                     }
                     inQuotedField = false;
                     continue;
                  }

                  //Should be the beginning of a quoted field...
                  for (int i = 0; i < sb.Length; i++) if (sb[i] != ' ') throwInvalidChar(ch);

                  sb.Length = 0;
                  inQuotedField = true;
                  goto NEXT_CHAR;
               }

               sb.Append((char)ch);


            NEXT_CHAR:
               ch = reader.Read();
            }
         EOR:
            nextChar = ch;
            if (line == 0 && SkipHeader) continue;
            if (fields.Count > 0) return true;
            if (this.SkipEmptyRecords) continue;
            return true;
         }
      }

      private void throwInvalidChar(int ch)
      {
         nextChar = -1;
         StringBuilder sb = new StringBuilder();
         sb.AppendFormat("Error at line {0}: unexpected char '{1}' (0x{1:X}), field={2}.", line, (char)ch, fields.Count);
         if (fileName != null)
         {
            sb.Append("\r\nFile=");
            sb.Append(fileName);
            sb.Append('.');
         }
         throw new Exception(sb.ToString());
      }
      private void throwError (String msg)
      {
         nextChar = -1;
         if (fileName != null) msg = msg + "\r\nFile=" + fileName + '.';
         throw new Exception(msg);
      }

      private void addFieldAndClear(StringBuilder sb, bool trim)
      {
         if (sb.Length == 0)
         {
            fields.Add(String.Empty);
            return;
         }
         if (!trim)
         {
            fields.Add(sb.ToString());
            goto CLEAR;
         }

         int i, j;
         for (i = 0; i < sb.Length; i++) if (sb[i] != ' ') break;
         for (j = sb.Length; j > i; j--) if (sb[j - 1] != ' ') break;
         if (j > i)
         {
            fields.Add(sb.ToString(i, j - i));
            goto CLEAR;
         }

         fields.Add(String.Empty);
      CLEAR:
         sb.Length = 0;
      }
   }

}
