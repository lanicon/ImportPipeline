using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Xml;
using System.IO;
using System.Diagnostics;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class ShellStreamDirectory : StreamDirectory
   {
      public readonly ShellStreamProvider provider;
      public ShellStreamDirectory(PipelineContext ctx, XmlElement providerNode, XmlElement parentNode)
         : base(ctx, providerNode)
      {
         provider = new ShellStreamProvider(ctx, providerNode, parentNode, this);
      }

      public override IEnumerator<object> GetChildren(PipelineContext ctx)
      {
         yield return provider;
      }

      public override string ToString()
      {
         return String.Format("{0} [url={1}]", GetType().Name, provider.Uri);
      }

   }



   public class ShellStreamProvider : StreamProvider
   {
      public bool KeepAlive;
      public String Command;
      public String Arguments;
      ProcessResultTypes resultTypes;
      public int[] NullStreamErrorCodes;
      public int[] AcceptedStreamErrorCodes;
      public bool ViaShell;

      public ShellStreamProvider(PipelineContext ctx, XmlNode node, XmlNode parentNode, StreamDirectory parent)
         : base(parent, node)
      {
         if (parentNode == null) parentNode = node;
         silent = (ctx.ImportFlags & _ImportFlags.Silent) != 0;

         uri = new Uri("cmd://something");

         resultTypes = new ProcessResultTypes();
         resultTypes.Add(0, 0);
         resultTypes.Add(node.ReadStr("@ignore_errors", null), 1);
         resultTypes.Add(node.ReadStr("@ok_errors", null), 0);


         Command = node.ReadStr("@cmd");
         Arguments = node.ReadStr("@args", null);
         ViaShell = node.ReadBool("@viashell", false);
         if (ViaShell && Arguments != null) throw new BMNodeException(node, "Attributes @args cannot be specified when @viashell=true.");
         CreateStream();
      }
      //public ShellStreamProvider(PipelineContext ctx, StreamProvider other, String url)
      //   : base(other)
      //{
      //   uri = new Uri(url);
      //   fullName = uri.ToString();
      //}


      public override Stream CreateStream()
      {
         using (var p = new ProcessHelper(this.Command, this.Arguments, ViaShell))
         {
            Stream x, y;
            p.SetStreams(x = new MemoryStream(), y = new MemoryStream(), true);
            int rc = p.Run();

            switch (resultTypes.HowToHandle(rc, 1))
            {
               case 0: break; //OK
               case 1: //Ignore
                  x = new MemoryStream();
                  break;
               default:
                  p.ThrowError();
                  break;
            }
            return x;
         }
      }



      /// <summary>
      /// Helper class to start a process, with or without redirected output
      /// </summary>
      public class ProcessHelper : IDisposable
      {
         public ProsessStreamProcessorBase StdErr { get; internal set; }
         public ProsessStreamProcessorBase StdOut { get; internal set; }
         Process process;
         ProcessStartInfo psi;
         private readonly bool viaShell;

         public ProcessHelper(String cmd, String args, bool viaShell)
         {
            this.viaShell = viaShell;
            process = new Process();
            psi = process.StartInfo;
            psi.Arguments = args;
            psi.FileName = cmd;
            psi.UseShellExecute = false;
            psi.LoadUserProfile = false;
            psi.WindowStyle = ProcessWindowStyle.Hidden;

            if (viaShell)
            {
               StringBuilder sb = new StringBuilder(256);
               sb.Append("/c ");
               formatCmd(sb, cmd, args);
               psi.Arguments = sb.ToString();
               psi.FileName = Environment.GetEnvironmentVariable("comspec");
            }
         }

         protected StringBuilder formatCmd(StringBuilder sb, String cmd, String args)
         {
            bool mustQuote = !String.IsNullOrEmpty(args) && !String.IsNullOrEmpty(cmd) && cmd[0] != '"';
            if (mustQuote) sb.Append('"');
            sb.Append(cmd);
            if (mustQuote) sb.Append('"');
            sb.Append(' ');

            sb.Append(args);
            return sb;
         }

         public StringBuilder FormatError(StringBuilder sb)
         {
            if (sb == null) sb = new StringBuilder();
            sb.Append("Command [");
            if (viaShell)
               sb.Append(psi.Arguments);
            else
               formatCmd(sb, psi.FileName, psi.Arguments);
            sb.AppendFormat("] failed. Returncode={0}.", this.process.ExitCode);
            if (StdErr != null)
            {
               sb.Append("Error text: ");
               sb.Append(StdErr.ReadAll());
            }
            return sb;
         }

         public void ThrowError()
         {
            throw new BMException(FormatError(null).ToString());
         }

         public void Start()
         {
            if (StdOut != null)
            {
               process.OutputDataReceived += StdOut.OnData;
               psi.RedirectStandardOutput = true;
               psi.StandardOutputEncoding = StdOut.encoding;
            }
            if (StdErr != null)
            {
               process.ErrorDataReceived += StdErr.OnData;
               psi.RedirectStandardError = true;
               psi.StandardErrorEncoding = StdErr.encoding;
            }

            process.Start();

            if (StdOut != null)
               process.BeginOutputReadLine();
            if (StdErr != null)
               process.BeginErrorReadLine();
         }

         public int WaitForExitAndFinalize()
         {
            process.WaitForExit();

            if (StdOut != null) StdOut.Reset();
            if (StdErr != null) StdErr.Reset();
            return process.ExitCode;
         }

         public int Run()
         {
            Start();
            return WaitForExitAndFinalize();
         }

         /// <summary>
         /// Initialize redirected streams by redirecting them to a stream
         /// </summary>
         public void SetStreams(Stream strmOut, Stream strmErr, bool errorToBoth)
         {
            SetStreams(strmOut, strmErr, errorToBoth, Encoding.UTF8);
         }

         /// <summary>
         /// Initialize redirected streams by redirecting them to a stream
         /// </summary>
         public void SetStreams(Stream strmOut, Stream strmErr, bool errorToBoth, Encoding enc)
         {
            if (strmOut == null && strmErr == null)
            {
               this.StdErr = null;
               this.StdOut = null;
               return;
            }

            if (strmOut != null)
            {
               this.StdOut = new ProsessStreamProcessor(strmOut, enc);
            }

            if (errorToBoth)
            {
               if (strmOut == null && StdOut != null) strmOut = ((ProsessStreamProcessor)StdOut).Strm;

               if (strmErr != null)
               {
                  this.StdErr = new ProsessStreamProcessor(strmErr, strmOut, enc);

               }
            }
            else
            {
               if (strmErr != null)
                  this.StdErr = new ProsessStreamProcessor(strmErr, enc);
            }

         }


         /// <summary>
         /// Initialize redirected streams by redirecting them to a string buffer
         /// </summary>
         public void SetBuffers(StringBuilder sbOut, StringBuilder sbErr, bool errorToBoth)
         {
            SetBuffers(sbOut, sbErr, errorToBoth, Encoding.UTF8);
         }

         /// <summary>
         /// Initialize redirected streams by redirecting them to a string buffer
         /// </summary>
         public void SetBuffers(StringBuilder sbOut, StringBuilder sbErr, bool errorToBoth, Encoding enc)
         {
            if (sbOut == null && sbErr == null)
            {
               this.StdErr = null;
               this.StdOut = null;
               return;
            }

            if (sbOut != null)
            {
               this.StdOut = new ProsessStringStreamProcessor(sbOut, enc);
            }

            if (errorToBoth)
            {
               if (sbOut == null && StdOut != null) sbOut = ((ProsessStringStreamProcessor)StdOut).Buffer;

               if (sbErr != null)
               {
                  this.StdErr = new ProsessStringStreamProcessor(sbErr, sbOut, enc);

               }
            }
            else
            {
               if (sbErr != null)
                  this.StdErr = new ProsessStringStreamProcessor(sbErr, enc);
            }

         }


         /// <summary>
         /// Baseclass for handling redirected output
         /// </summary>
         public abstract class ProsessStreamProcessorBase
         {
            public readonly Encoding encoding;

            protected ProsessStreamProcessorBase(Encoding enc)
            {
               encoding = enc;
            }

            public abstract void OnData(object sender, DataReceivedEventArgs e);

            public abstract String ReadAll();
            public abstract void Reset();
         }


         /// <summary>
         /// Processes redirected output by storing the output in a stream.
         /// </summary>
         public class ProsessStreamProcessor : ProsessStreamProcessorBase
         {
            public readonly Stream Strm;
            public readonly Stream SlaveStream;
            private byte[] buf;

            public ProsessStreamProcessor(Stream x, Encoding enc): base(enc)
            {
               Strm = x;
            }

            public ProsessStreamProcessor(Stream strmMaster, Stream strmSlave, Encoding enc): base (enc)
            {
               if (strmMaster == null)
                  Strm = strmSlave;
               else
               {
                  Strm = strmMaster;
                  if (strmSlave != null && strmSlave != strmMaster)
                     this.SlaveStream = strmSlave;
               }
            }

            public override void OnData(object sender, DataReceivedEventArgs e)
            {
               if (e.Data == null) return;
               int len = 2 + 2 * e.Data.Length;
               if (buf == null || buf.Length < len)
                  buf = new byte[len];
               len = encoding.GetBytes(e.Data, 0, e.Data.Length, buf, 0);
               buf[len] = 10; //lf
               Strm.Write(buf, 0, len);
               if (SlaveStream != null) SlaveStream.Write(buf, 0, len);
            }

            public override String ReadAll()
            {
               Strm.Position = 0;
               var sr = new StreamReader(Strm, encoding);
               return sr.ReadToEnd();
            }
            public override void Reset()
            {
               Strm.Position = 0; 
            }
         }


         /// <summary>
         /// Processes redirected output by storing the output in a StringBuilder
         /// </summary>
         public class ProsessStringStreamProcessor : ProsessStreamProcessorBase
         {
            public readonly StringBuilder Buffer;
            public readonly StringBuilder SlaveBuffer;
            private byte[] buf;

            public ProsessStringStreamProcessor(StringBuilder sb, Encoding enc)
               : base(enc)
            {
               Buffer = sb;
            }

            public ProsessStringStreamProcessor(StringBuilder sbMaster, StringBuilder sbSlave, Encoding enc)
               : base(enc)
            {
               if (sbMaster == null)
                  Buffer = sbMaster;
               else
               {
                  Buffer = sbMaster;
                  if (sbSlave != null && sbSlave != sbMaster)
                     this.SlaveBuffer = sbSlave;
               }
            }

            public override void OnData(object sender, DataReceivedEventArgs e)
            {
               if (e.Data == null) return;
               Buffer.AppendLine(e.Data);
               if (SlaveBuffer != null) SlaveBuffer.AppendLine(e.Data); ;
            }

            public override String ReadAll()
            {
               return Buffer.ToString();
            }

            public override void Reset()
            {
            }

         }

         public void Dispose()
         {
            Utils.FreeAndNil(ref process);
         }
      }

      /// <summary>
      /// Determines how an exitcode should be handled.
      /// The howToHandle per exitcode is completely client determined
      /// </summary>
      public class ProcessResultTypes
      {
         private Dictionary<int, int> handlers;

         public ProcessResultTypes()
         {
            handlers = new Dictionary<int, int>();
         }
         public void Add(int rc, int howToHandle)
         {
            handlers[rc] = howToHandle;
         }

         public void Add(int[] rcs, int howToHandle)
         {
            foreach (int rc in rcs) handlers[rc] = howToHandle;
         }

         public void Add(String str, int howToHandle)
         {
            foreach (var rcStr in str.SplitStandard())
            {
               handlers[Invariant.ToInt32(rcStr)] = howToHandle;
            }
         }

         public int HowToHandle(int rc, int def)
         {
            int ret;
            return handlers.TryGetValue(rc, out ret) ? ret : def;
         }
      }
   }
}