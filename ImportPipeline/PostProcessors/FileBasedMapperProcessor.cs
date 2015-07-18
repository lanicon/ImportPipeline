using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Json;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;

namespace Bitmanager.ImportPipeline
{
//   <postprocessor name=abc type=fileBaseMapper>
//   <dir name=temp keepfiles=false compress=true />
//   <hasher>
//      <key expr=boe type=int />
//      <key expr=boe type=int />
//   </hasher>
//</postprocessor>

   public class FileBasedMapperProcessor : PostProcessorBase
   {
      private readonly String directory;
      private readonly JHasher hasher;
      private readonly JComparer comparer;

      private readonly int numFiles;
      private readonly int maxNullIndex;
      private readonly bool keepFiles, compress;

      private FileBasedMapperWriters mapper = null;

      public FileBasedMapperProcessor(ImportEngine engine, XmlNode node): base (engine, node)
      {
         directory = engine.Xml.CombinePath(node.ReadStr("dir/@name"));
         keepFiles = node.ReadBool("dir/@keepfiles", false);
         compress = node.ReadBool("dir/@compress", true);
         maxNullIndex = node.ReadInt("dir/@max_null_index", -1);
         numFiles = node.ReadInt("dir/@count", 100);
         if (numFiles <= 0) throw new BMNodeException(node, "Count should be > 0.");

         XmlNode sortNode = node.SelectSingleNode("sorter");
         bool sameAsHash = (sortNode != null && sortNode.ReadBool("@sameashash", false));

         List<KeyAndType> list = KeyAndType.CreateKeyList(node.SelectMandatoryNode("hasher"), "key", true, sameAsHash);
         hasher = JHasher.Create(list);

         if (sortNode != null)
         {
            if (!sameAsHash)
               list = KeyAndType.CreateKeyList(sortNode, "key", true, true);
            comparer = JComparer.Create(list);
         }
      }

      public FileBasedMapperProcessor(FileBasedMapperProcessor other, IDataEndpoint epOrnextProcessor)
         : base(other, epOrnextProcessor)
      {
         directory = other.directory;
         hasher = other.hasher;
         comparer = other.comparer;
         keepFiles = other.keepFiles;
         numFiles = other.numFiles;
         compress = other.compress;
         maxNullIndex = other.maxNullIndex;
      }


      public override IPostProcessor Clone(IDataEndpoint epOrnextProcessor)
      {
         return new FileBasedMapperProcessor(this, epOrnextProcessor);
      }

      public override void CallNextPostProcessor(PipelineContext ctx)
      {
         if (mapper!=null)
         {
            foreach (var o in mapper)
            {
               nextEndpoint.SetField(null, o);
               nextEndpoint.Add(ctx);
            }
         }

         base.CallNextPostProcessor(ctx);
      }


      public override void Add(PipelineContext ctx)
      {
         if (mapper == null) mapper = new FileBasedMapperWriters(hasher, comparer, directory, Name, numFiles, compress, keepFiles);
         if (accumulator.Count > 0)
         {
            if (!mapper.OptWrite(accumulator, maxNullIndex))
            {
               //Just passthrough to the next endpoint if this record had a failing hash-value
               nextEndpoint.SetField(null, accumulator);
               nextEndpoint.Add(ctx);
            }
            Clear();
         }
      }
   }
}
