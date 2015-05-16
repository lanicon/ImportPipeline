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
   }

   public interface IStreamProvidersRoot
   {
      void Init(PipelineContext ctx, XmlNode node);
      IEnumerable<IStreamProviderBase> GetRootElements(PipelineContext ctx);
      IEnumerable<IStreamProvider> GetElements(PipelineContext ctx);
   }

}
