using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using System.Reflection;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public interface IOutputStreamProvider
   {
      XmlNode ContextNode { get; }
      Stream CreateStream();
      CredentialCache Credentials { get; }
   }

   public abstract class OutputStreamProviderBase : StreamProviderBase, IOutputStreamProvider
   {
      public OutputStreamProviderBase (PipelineContext ctx, XmlNode contextNode): base (contextNode, null)
      {
      }
      public virtual XmlNode ContextNode
      {
         get { return contextNode; }
      }

      public abstract Stream CreateStream();

      public virtual CredentialCache Credentials
      {
         get { return base.GetCredentials(); }
      }

      public virtual NetworkCredential Credential
      {
         get { return base.GetCredential(); }
      }

      public static IOutputStreamProvider Create(PipelineContext ctx, XmlNode contextNode)
      {
         if (contextNode.SelectSingleNode("@file") != null)
            return new FileOutputStreamProvider(ctx, contextNode);
         throw new BMException("Cannot create OutputStreamProvider: unknown.");
      }
   }

   public class FileOutputStreamProvider : OutputStreamProviderBase
   {
      public readonly String FileName;
      public FileOutputStreamProvider(PipelineContext ctx, XmlNode contextNode)
         : base(ctx, contextNode)
      {
         FileName = contextNode.ReadPath("@file");
      }
      public override Stream CreateStream()
      {
         return IOUtils.CreateOutputStream(FileName);
      }
   }


   //public class WebOutputStreamProvider : OutputStreamProviderBase
   //{
   //   public WebOutputStreamProvider(PipelineContext ctx, XmlNode contextNode)
   //      : base(ctx, contextNode)
   //   {
   //   }
   //}
}
