using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Windows.Forms;
using Bitmanager.Importer;
using Bitmanager.IO;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public enum StreamProtocol {File, Http, Other};

   public interface IStreamProviderBase
   {
      IStreamProviderBase Parent { get; }
      XmlNode ContextNode { get; }
   }

   public interface IStreamProviderCollection : IStreamProviderBase
   {
      IEnumerable<IStreamProviderBase> GetElements(PipelineContext ctx);
   }

   public interface IStreamProvider : IStreamProviderBase
   {
      String VirtualName { get; }
      String VirtualRoot { get; }
      String RelativeName { get; }
      String FullName { get; }
      Uri Uri { get; }
      Stream CreateStream();
      CredentialCache Credentials { get; }
      DateTime LastModified { get; }
      long Size { get; }

      //public static Object EmitSubValues (IStreamProvider p, PipelineContext ctx, String pfx)
      //{
      //   if (String.IsNullOrEmpty(pfx)) 
      //      pfx = "_item/_start/";
      //   else
      //   {
      //      if (pfx[pfx.Length-1]!='/') pfx += '/';
      //   }

      //   Object tmp;
      //   Pipeline pipeline = ctx.Pipeline;
      //   Object ret = pipeline.HandleValue(ctx, pfx + "FullName", p.FullName);
      //   tmp = pipeline.HandleValue(ctx, pfx + "VirtualName", p.VirtualName);
      //   if (ret == null) ret = tmp;
      //   tmp = pipeline.HandleValue(ctx, pfx + "RelativeName", p.RelativeName);
      //   if (ret == null) ret = tmp;
      //   tmp = pipeline.HandleValue(ctx, pfx + "Uri", p.Uri);
      //   if (ret == null) ret = tmp;
      //   tmp = pipeline.HandleValue(ctx, pfx + "LastModified", p.LastModified);
      //   if (ret == null) ret = tmp;
      //   return ret != null ? ret : tmp;
      //}
   }

   public interface IStreamProvidersRoot
   {
      void Init(PipelineContext ctx, XmlNode node);
      IEnumerable<IStreamProviderBase> GetRootElements(PipelineContext ctx);
      IEnumerable<IStreamProvider> GetElements(PipelineContext ctx);
   }

}
