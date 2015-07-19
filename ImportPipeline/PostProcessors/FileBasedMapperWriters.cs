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
            writers[i] = createWriter(fileNames[i], compress, keepFiles);
         }
      }

      private static StreamWriter createWriter(String fn, bool compress, bool keepFiles)
      {
         FileOptions options = FileOptions.SequentialScan;
         if (!keepFiles) options |= FileOptions.DeleteOnClose;

         Stream strm = new FileStream(fn, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 4096, options);
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

      private StreamReader createReaderFromWriter(StreamWriter wtr, int idx)
      {
         wtr.Flush();
         Stream x = wtr.BaseStream;
         var zipStream = x as GZipStream;
         if (zipStream == null)
         {
            if (wtr.BaseStream == null) return null;
            wtr.BaseStream.Position = 0;
            return wtr.BaseStream.CreateTextReader();
         }

         //We had a zip-stream. Close the writing stream, and create a zip reader
         x = zipStream.BaseStream;
         if (x == null) return null;
         zipStream.Close();
         x.Position = 0;
         zipStream = new GZipStream(x, CompressionMode.Decompress, true);

         return zipStream.CreateTextReader();
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
      public bool OptWrite(JObject data, int maxNullIndex = -1)
      {
         int nullIndex;
         uint hash = (uint)hasher.GetHash(data, out nullIndex);
         if (nullIndex > maxNullIndex) return false;
         uint file = (hash % (uint)writers.Length);
         var wtr = writers[file];
         data.WriteTo(wtr, Newtonsoft.Json.Formatting.None);
         wtr.WriteLine();
         return true;
      }

      public void Dispose()
      {
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
         //if (keepFiles) return;
         //for (int i = 0; i < fileNames.Length; i++)
         //{
         //   var name = fileNames[i];
         //   if (name == null) continue;
         //   fileNames[i] = null;
         //   try
         //   {
         //      File.Delete(name);
         //   }
         //   catch (Exception e)
         //   {
         //      Logs.ErrorLog.Log("Cannot delete {0}: {1}", fileNames[i], e.Message);
         //   }
         //}
      }

      public FileBasedMapperEnumerator GetObjectEnumerator(int index)
      {
         if (index < 0 || index >= writers.Length) return null;
         String fn = fileNames[index];
         var wtr = writers[index];
         if (wtr == null) throw new BMException ("File already closed: {0}.", fn);

         var rdr = createReaderFromWriter(wtr, index);
         if (rdr == null) throw new BMException("File cannot be enumerator more than once. File={0}.", fn);
         return comparer == null ? new FileBasedMapperUnsortedEnumerator(rdr, fn, index) : new FileBasedMapperSortedEnumerator(rdr, fn, index, comparer);
      }

      public IEnumerator<JObject> GetEnumerator()
      {
         Logger lg = Logs.CreateLogger("import", "script");
         for (int index = 0; index < writers.Length; index++)
         {
            var e = GetObjectEnumerator(index);
            try 
            {
               while (true)
               {
                  var obj = e.GetNext();
                  if (obj==null) break;
                  yield return obj;
               }
            }
            finally
            {
               Utils.FreeAndNil (ref e);
            }
         }
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }
   }

   public abstract class FileBasedMapperEnumerator : IDisposable//, IEnumerable<JObject>
   {
      protected String readerFile;
      protected int index;
      public FileBasedMapperEnumerator(String filename, int index)
      {
         this.index = index;
         //this.reader = rdr;
         this.readerFile = filename;
      }

      public abstract JObject GetNext();
      public abstract void Close();

      public abstract void Dispose();
   }

   public class FileBasedMapperUnsortedEnumerator : FileBasedMapperEnumerator
   {
      private StreamReader reader;
      public FileBasedMapperUnsortedEnumerator(StreamReader rdr, String filename, int index): base (filename, index)
      {
         this.reader = rdr;
      }

      public override JObject GetNext()
      {
         String line = reader.ReadLine();
         return (line == null) ? null : (JObject)JToken.Parse(line);
      }

      public override void Close()
      {
         closeReader();
      }



      public override void Dispose()
      {
         closeReader();
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
   }
   public class FileBasedMapperSortedEnumerator : FileBasedMapperUnsortedEnumerator
   {
      private JComparer sorter;
      private List<JObject> buffer;
      private int bufferIndex, bufferLast;
      public FileBasedMapperSortedEnumerator(StreamReader rdr, String filename, int index, JComparer sorter) :  base (rdr, filename, index)
      {
         this.sorter = sorter;
         buffer = new List<JObject>(7000);
         while (true)
         {
            var o = base.GetNext();
            if (o == null) break;
            buffer.Add(o);
         }
         buffer.Sort(sorter);
         bufferLast = buffer.Count - 1;
         bufferIndex = -1;
      }

      public override JObject GetNext()
      {
         if (bufferIndex >= bufferLast) return null;
         return buffer[++bufferIndex];
      }
   }
}
