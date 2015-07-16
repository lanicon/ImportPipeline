using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline
{
   public class FileBasedMapperWriters : IDisposable, IEnumerable<JObject>
   {
      readonly JComparer comparer;
      readonly JHasher hasher;
      String[] fileNames;
      StreamWriter[] writers;
      StreamReader reader;
      String readerFile;
      private readonly bool compress, keepFiles;

      public FileBasedMapperWriters(JHasher hasher, JComparer comparer, String dir, String id, int cnt, bool compress, bool keepFiles=false)
      {
         this.hasher = hasher;
         this.comparer = comparer;
         this.compress = compress;
         this.keepFiles = keepFiles;
         fileNames = new String[cnt];
         writers = new StreamWriter[cnt];

         IOUtils.ForceDirectories(dir, false);
         String part1 = Path.GetFullPath(dir + "\\" + id);
         dir = IOUtils.DelSlash(part1);
         for (int i = 0; i < cnt; i++)
         {
            fileNames[i] = String.Format("{0}_{1}.tmp", part1, i);
            writers[i] = createWriter(fileNames[i], compress);
         }
      }

      private static StreamWriter createWriter(String fn, bool compress)
      {
         Stream strm = new FileStream(fn, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 4096);
         try
         {
            if (compress)
            {
               strm = new GZipStream(strm, CompressionLevel.Fastest, true);
            }
            return strm.CreateTextWriter();
         }
         catch
         {
            strm.Close();
            throw;
         }
      }

      private static void closeWriter(StreamWriter wtr)
      {
         Stream x = wtr.BaseStream;
         var zipStream = x as GZipStream;
         if (zipStream == null)
         {
            wtr.Close();
            return;
         }
         x = zipStream.BaseStream;
         zipStream.Close();
         x.Close();
      }
      private void closeReader()
      {
         if (reader == null) return;
         var rdr = reader;
         reader = null;
         try
         {
            Stream x = rdr.BaseStream;
            var zipStream = x as GZipStream;
            if (zipStream == null)
            {
               rdr.Close();
               return;
            }
            x = zipStream.BaseStream;
            zipStream.Close();
            x.Close();
         }
         catch (Exception e)
         {
            Logs.ErrorLog.Log("Cannot close {0}: {1}", readerFile, e.Message);
         }
      }

      private StreamReader createReaderFromWriter(StreamWriter wtr, int idx)
      {
         closeReader();
         readerFile = this.fileNames[idx];
         wtr.Flush();
         Stream x = wtr.BaseStream;
         var zipStream = x as GZipStream;
         if (zipStream == null)
         {
            wtr.BaseStream.Position = 0;
            return reader = wtr.BaseStream.CreateTextReader();
         }

         //We had a zip-stream. Close the writing stream, and create a zip reader
         x = zipStream.BaseStream;
         zipStream.Close();
         x.Position = 0;
         zipStream = new GZipStream(x, CompressionMode.Decompress, true);

         return reader = zipStream.CreateTextReader();
      }

      /// <summary>
      /// Writes the data to the appropriate file (designed by the hash)
      /// </summary>
      public void Write(JObject data)
      {
         uint hash = (uint)hasher.GetHash(data);
         uint file = (hash % (uint)writers.Length);
         var wtr = writers[file];
         data.WriteTo(wtr, Newtonsoft.Json.Formatting.None);
         wtr.WriteLine();
      }

      /// <summary>
      /// Optional writes the data to the appropriate file (designed by the hash)
      /// If the index of the first key that had a null value >= minNullIndex, the value is not written.
      /// 
      /// The returnvalue reflects whether has been written or not.
      /// </summary>
      public bool OptWrite(JObject data, int minNullIndex = 0)
      {
         int nullIndex;
         uint hash = (uint)hasher.GetHash(data, out nullIndex);
         if (nullIndex >= minNullIndex) return false;
         uint file = (hash % (uint)writers.Length);
         var wtr = writers[file];
         data.WriteTo(wtr, Newtonsoft.Json.Formatting.None);
         wtr.WriteLine();
         return true;
      }

      public void Dispose()
      {
         closeReader();
         for (int i = 0; i < writers.Length; i++)
         {
            var wtr = writers[i];
            if (wtr == null) continue;
            writers[i] = null;
            try
            {
               closeWriter(wtr);
            }
            catch (Exception e)
            {
               Logs.ErrorLog.Log("Cannot close {0}: {1}", fileNames[i], e.Message);
            }
         }
         if (keepFiles) return;
         for (int i = 0; i < fileNames.Length; i++)
         {
            var name = fileNames[i];
            if (name == null) continue;
            fileNames[i] = null;
            try
            {
               File.Delete(name);
            }
            catch (Exception e)
            {
               Logs.ErrorLog.Log("Cannot delete {0}: {1}", fileNames[i], e.Message);
            }
         }
      }

      int curReadFile = -1;
      //JToken curFrbr = null;
      //public String DiagnosticInfo { get { return curReadFile < 0 ? null : String.Format("{0}, [frbr {1}].", this.fileNames[curReadFile], curFrbr); } }

      public IEnumerator<JObject> GetEnumerator()
      {
         Logger lg = Logs.CreateLogger("import", "script");
         for (curReadFile = 0; curReadFile < writers.Length; curReadFile++)
         {

            var wtr = writers[curReadFile];
            if (wtr == null) continue;

            closeReader();
            var textRdr = createReaderFromWriter(wtr, curReadFile);
            if (textRdr.EndOfStream) continue;

            List<JObject> objects = new List<JObject>(70000);
            while (true)
            {
               String line = textRdr.ReadLine();
               if (line == null) break;
               objects.Add((JObject)JToken.Parse(line));
            }
            if (comparer != null) objects.Sort(comparer);
            //curFrbr = null;
            for (int j = 0; j < objects.Count; j++)
            {
               var tmp = objects[j];
               //curFrbr = tmp["frbr"];
               yield return tmp;
            }
         }
         closeReader();
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }
   }

}
