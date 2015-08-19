﻿using Bitmanager.Core;
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
   public abstract class MappedObjectEnumerator : IDisposable
   {
      protected String readerFile;
      protected int index;
      public MappedObjectEnumerator(String filename, int index)
      {
         this.index = index;
         //this.reader = rdr;
         this.readerFile = filename;
      }

      public abstract JObject GetNext();
      public abstract List<JObject> GetAll();
      public abstract void Close();

      public abstract void Dispose();
   }

   public abstract class MapperWritersBase : IDisposable
   {
      //static Logger dbgLogger = null;//Logs.CreateLogger("import", "map");
      //readonly JComparer comparer;
      //readonly JComparer hasher;
      //String[] fileNames;
      //StreamWriter[] writers;
      //private readonly bool compress, keepFiles;

      //public FileBasedMapperWriters(JComparer hasher, JComparer comparer, String dir, String id, int cnt, bool compress, bool keepFiles = false)
      //{
      //   this.hasher = hasher;
      //   this.comparer = comparer;
      //   this.compress = compress;
      //   this.keepFiles = keepFiles;
      //   fileNames = new String[cnt];
      //   writers = new StreamWriter[cnt];

      //   IOUtils.ForceDirectories(dir, false);
      //   String part1 = Path.GetFullPath(dir + "\\" + id);
      //   dir = IOUtils.DelSlash(part1);
      //   for (int i = 0; i < cnt; i++)
      //   {
      //      fileNames[i] = String.Format("{0}_{1}.tmp", part1, i);
      //      writers[i] = createWriter(fileNames[i], compress, keepFiles);
      //   }
      //}


      /// <summary>
      /// Writes the data to the appropriate file (designed by the hash)
      /// </summary>
      public abstract void Write(JObject data);


      /// <summary>
      /// Optional writes the data to the appropriate file (designed by the hash)
      /// If the index of the first key that had a null value >= minNullIndex, the value is not written.
      /// 
      /// The returnvalue reflects whether has been written or not.
      /// </summary>
      public abstract bool OptWrite(JObject data, int maxNullIndex = -1);

      public abstract void Dispose();

      public abstract MappedObjectEnumerator GetObjectEnumerator(int index, bool buffered = false);
   }
}