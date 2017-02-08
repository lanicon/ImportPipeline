using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Importer;
using Renci.SshNet;
using System.Net;


namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class SSHStreamDirectory : StreamDirectory
   {
      public readonly SSHStreamProvider provider;
      public SSHStreamDirectory(PipelineContext ctx, XmlElement providerNode, XmlElement parentNode)
         : base(ctx, providerNode)
      {
         provider = new SSHStreamProvider(ctx, providerNode, parentNode, this);
      }

      public override IEnumerator<object> GetChildren(PipelineContext ctx)
      {
         yield return provider;
      }

      public override string ToString()
      {
         return String.Format ("{0} [url={1}]", GetType().Name, provider.Uri);
      }

   }
   public class SSHStreamProvider : StreamProvider
   {
      public SshClient sshClient;
      public SSHStreamProvider(PipelineContext ctx, XmlNode node, XmlNode parentNode, StreamDirectory parent)
         : base(parent, node)
      {
         credentialsNeeded = true;
         if (parentNode == null) parentNode = node;
         silent = (ctx.ImportFlags & _ImportFlags.Silent) != 0;

         uri = new Uri("ssh://192.168.76.134/");
         createClient();
      }
      public SSHStreamProvider(PipelineContext ctx, StreamProvider other, String url)
         : base(other)
      {
         uri = new Uri(url);
         fullName = uri.ToString();
      }

      private SshClient createClient()
      {
         if (sshClient != null) return sshClient;
         NetworkCredential cred = base.Credential;
         List<AuthenticationMethod> authMethods = new List<AuthenticationMethod>();
         authMethods.Add (new Renci.SshNet.PasswordAuthenticationMethod(cred.UserName, cred.Password));

         var conn = new ConnectionInfo("192.168.76.134", cred.UserName, authMethods.ToArray());
         using (var ssh = new SshClient(conn))
         {
            ssh.Connect();
            var command = ssh.CreateCommand("ls -!");
            var result = command.Execute();
            Console.Out.WriteLine(result);
            ssh.Disconnect();
         }
         return null;
      }

      //protected virtual void onPrepareRequest(HttpWebRequest req)
      //{
      //   PrepareRequest(req);
      //}
      //public virtual void PrepareRequest(HttpWebRequest req)
      //{
      //   req.KeepAlive = KeepAlive;
      //   req.Credentials = Credentials;
      //}
      public ConnectionInfo CreateConnectionInfo()
      {
         const string privateKeyFilePath = @"C:\some\private\key.pem";
         ConnectionInfo connectionInfo;
         using (var stream = new FileStream(privateKeyFilePath, FileMode.Open, FileAccess.Read))
         {
            var privateKeyFile = new PrivateKeyFile(stream);
            AuthenticationMethod authenticationMethod =
                new PrivateKeyAuthenticationMethod("ubuntu", privateKeyFile);

            connectionInfo = new ConnectionInfo(
                "my.server.com",
                "ubuntu",
                authenticationMethod);
         }

         return connectionInfo;
      }


      public void Connect()
      {
         using (var ssh = new SshClient(CreateConnectionInfo()))
         {
            ssh.Connect();
            var command = ssh.CreateCommand("uptime");
            var result = command.Execute();
            Console.Out.WriteLine(result);
            ssh.Disconnect();
         }
      }



      //public void GetConfigurationFiles()
      //{
      //   using (var scp = new ScpClient(CreateNginxServerConnectionInfo()))
      //   {
      //      scp.Connect();

      //      scp.Download("/etc/nginx/", new DirectoryInfo(@"D:\Temp\ScpDownloadTest"));

      //      scp.Disconnect();
      //   }
      //}

      public override Stream CreateStream()
      {
         base.InitCredentials();
         var cache = base.credentialCache;
         NetworkCredential cred = null;
         foreach (NetworkCredential c in cache)
         {
            cred = c;
            break;
         }

         List<AuthenticationMethod> authMethods = new List<AuthenticationMethod>();
         ConnectionInfo ci = new ConnectionInfo("host", cred.UserName);
         return null;
         //HttpWebRequest req = (HttpWebRequest)HttpWebRequest.Create(uri);
         //PrepareRequest(req);
         ////if (Timeout > 0 || Timeout == -1)
         ////{
         ////   req.Timeout = Timeout;
         ////   req.ReadWriteTimeout = Timeout;
         ////}
         //HttpWebResponse resp;
         //try
         //{
         //   resp = (HttpWebResponse)req.GetResponse();
         //}
         //catch (WebException we)
         //{
         //   throw new BMWebException(we, uri);
         //}
         //return new WebStreamWrapper(resp);
      }



   }
}
