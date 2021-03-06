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
   public class FileBasedMapperWriters : MapperWritersBase, IEnumerable<JObject>
   {
      static Logger dbgLogger = null;//Logs.CreateLogger("import", "map");
      readonly JComparer comparer;
      readonly JComparer hasher;
      String[] fileNames;
      StreamWriter[] writers;
      private readonly bool compress, keepFiles;

      public FileBasedMapperWriters(JComparer hasher, JComparer comparer, String dir, String id, int cnt, bool compress, bool keepFiles = false)
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
         if (dbgLogger != null) dbgLogger.Log("createReaderFromWriter ({0}, {1})", idx, fileNames[idx]);
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
         zipStream = new GZipStream(x, CompressionMode.Decompress, false);

         return zipStream.CreateTextReader();
      }

      /// <summary>
      /// Writes the data to the appropriate file (designed by the hash)
      /// </summary>
      public override void Write(JObject data)
      {
         uint hash = (uint)hasher.GetHash(data);
         uint file = (hash % (uint)writers.Length);
         try
         {
            var wtr = writers[file];
            data.WriteTo(wtr, Newtonsoft.Json.Formatting.None);
            wtr.WriteLine();
         }
         catch (Exception e)
         {
            throw WrapWithFile(e, file);
         }
      }


      /// <summary>
      /// Optional writes the data to the appropriate file (designed by the hash)
      /// If the index of the first key that had a null value >= minNullIndex, the value is not written.
      /// 
      /// The returnvalue reflects whether has been written or not.
      /// </summary>
      public override bool OptWrite(JObject data, int maxNullIndex = -1)
      {
         int nullIndex;
         uint hash = (uint)hasher.GetHash(data, out nullIndex);
         if (nullIndex > maxNullIndex) return false;
         uint file = (hash % (uint)writers.Length);
         try
         {
            var wtr = writers[file];
            data.WriteTo(wtr, Newtonsoft.Json.Formatting.None);
            wtr.WriteLine();
            return true;
         }
         catch (Exception e)
         {
            throw WrapWithFile(e, file);
         }
      }

      private Exception WrapWithFile (Exception e, uint file)
      {
         return new BMException(e, "{0}\nFile #={1} name={2}.", e.Message, file, fileNames[file]);
      }


      public override void Dispose()
      {
         for (int i = 0; i < writers.Length; i++)
         {
            var wtr = writers[i];
            if (wtr == null) continue;
            writers[i] = null;
            try
            {
               if (dbgLogger != null) dbgLogger.Log("closeWriter ({0}, {1})", i, fileNames[i]);
               closeWriter(wtr);
            }
            catch (Exception e)
            {
               Logs.ErrorLog.Log("Cannot close {0}: {1}", fileNames[i], e.Message);
            }
         }
      }

      public override MappedObjectEnumerator GetObjectEnumerator(int index, bool buffered=false)
      {
         if (index < 0 || index >= writers.Length) return null;
         String fn = fileNames[index];
         var wtr = writers[index];
         if (wtr == null) throw new BMException ("File already closed: {0}.", fn);

         var rdr = createReaderFromWriter(wtr, index);
         if (rdr == null) throw new BMException("File cannot be enumerator more than once. File={0}.", fn);
         return (comparer != null || buffered) ?  new SortedObjectEnumerator(rdr, fn, index, comparer) : new UnbufferedObjectEnumerator(rdr, fn, index);
      }


      public IEnumerator<JObject> GetEnumerator()
      {
         for (int index = 0; index < writers.Length; index++)
         {
            var e = GetObjectEnumerator(index);
            try
            {
               while (true)
               {
                  var obj = e.GetNext();
                  if (obj == null) break;
                  yield return obj;
               }
            }
            finally
            {
               Utils.FreeAndNil(ref e);
            }
         }
      }

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }


      // --------------------------------------------------------------------------------------------- 
      // Enumerator objects
      // ---------------------------------------------------------------------------------------------
      public class UnbufferedObjectEnumerator : MappedObjectEnumerator
      {
         private StreamReader reader;
         public UnbufferedObjectEnumerator(StreamReader rdr, String filename, int index)
            : base(filename, index)
         {
            this.reader = rdr;
         }

         public override JObject GetNext()
         {
            String line = reader.ReadLine();
            return (line == null) ? null : (JObject)JToken.Parse(line);
         }
         public override List<JObject> GetAll()
         {
            var ret = new List<JObject>(4000);
            while (true)
            {
               String line = reader.ReadLine();
               if (line == null) break;
               ret.Add((JObject)JToken.Parse(line));
            }
            return ret;
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
      public class SortedObjectEnumerator : UnbufferedObjectEnumerator
      {
         private JComparer sorter;
         private List<JObject> buffer;
         private int bufferIndex, bufferLast;
         public SortedObjectEnumerator(StreamReader rdr, String filename, int index, JComparer sorter)
            : base(rdr, filename, index)
         {
            this.sorter = sorter;
            buffer = base.GetAll();
            if (sorter != null) buffer.Sort(sorter);
            bufferLast = buffer.Count - 1;
            bufferIndex = -1;
         }

         public override JObject GetNext()
         {
            if (bufferIndex >= bufferLast) return null;
            return buffer[++bufferIndex];
         }

         public override List<JObject> GetAll()
         {
            return buffer;
         }

      }

   }


}
