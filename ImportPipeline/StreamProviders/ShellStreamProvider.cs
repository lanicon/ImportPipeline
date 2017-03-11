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
using System.Threading;

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
      protected readonly ImportEngine engine;
      public bool KeepAlive;
      public String Command;
      public String Arguments;
      ProcessResultTypes resultTypes;
      public int[] NullStreamErrorCodes;
      public int[] AcceptedStreamErrorCodes;

      private ProcessHelper processHelper;
      private IAsyncResult dlgAsyncTerminatorResult;
      private Action<Stream> dlgOnClose;
      private Action dlgAsyncTerminator;
      private Exception processError;
      public bool ViaShell;
      bool buffered;
      bool stderrToStdout;

      public ShellStreamProvider(PipelineContext ctx, XmlNode node, XmlNode parentNode, StreamDirectory parent)
         : base(parent, node)
      {
         engine = ctx.ImportEngine;
         if (parentNode == null) parentNode = node;
         silent = (ctx.ImportFlags & _ImportFlags.Silent) != 0;

         uri = new Uri("cmd://something");

         resultTypes = new ProcessResultTypes();
         resultTypes.Add(0, 0);
         resultTypes.Add(node.ReadStr("@ignore_errors", null), 1);
         resultTypes.Add(node.ReadStr("@ok_errors", null), 0);

         buffered = node.ReadBool("@buffered", true);
         stderrToStdout = node.ReadBool("@stderr_to_stdout", false);

         Command = node.ReadStr("@cmd");
         Arguments = node.ReadStr("@args", null);
         ViaShell = node.ReadBool("@viashell", false);
         if (ViaShell && Arguments != null) throw new BMNodeException(node, "Attributes @args cannot be specified when @viashell=true.");
      }

      private void onClose (Stream x)
      {
         Utils.FreeAndNil(ref processHelper);
         if (processError != null)
         {
            throw new BMException(processError, processError.Message);
         }
      }

      private void asyncTerminator ()
      {
         if (processHelper == null) return;
         try
         {
            int rc = processHelper.WaitForExitAndFinalize();

            switch (resultTypes.HowToHandle(rc, 1))
            {
               case 0:  //OK
               case 1: //Ignore
                  break;
               default:
                  processHelper.ThrowError();
                  break;
            }
         }
         catch (Exception e)
         {
            processError = e;
         }
      }


      public override Stream CreateStream(PipelineContext ctx)
      {
         String workdir = ctx.ImportEngine.Xml.BaseDir;
         if (buffered)
         {
            using (var p=new ProcessHelper(this.Command, this.Arguments, ViaShell, workdir)) {
               Stream x, y;
               p.SetStreams(x = new MemoryStream(), y = new MemoryStream(), stderrToStdout);
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

         //Not buffered, but use the stream directly
         processHelper = new ProcessHelper(this.Command, this.Arguments, ViaShell, workdir);
         try
         {
            Stream x, y;
            processHelper.SetStreams(x = new ProcessPipeStream(onClose), y = new MemoryStream(), true);
            processHelper.Start();
            dlgAsyncTerminator = asyncTerminator;
            dlgAsyncTerminatorResult = dlgAsyncTerminator.BeginInvoke(null, null);
            return x;
         }
         catch (Exception e)
         {
            processHelper.Dispose();
            processHelper = null;
            throw;
         }
      }

      class MemoryStreamWrapper: MemoryStream
      {
         Action<Stream> onClose;

         public MemoryStreamWrapper (Action<Stream> onClose)
         {
            this.onClose = onClose;
         }

         public override void Close()
         {
            base.Close();
            if (onClose != null)
            {
               onClose(this);
            }
         }      


      }


      /// <summary>
      /// Helper class to start a process, with or without redirected output
      /// </summary>
      public class ProcessHelper : IDisposable
      {
         public ProsessStreamProcessorBase StdErr { get; internal set; }
         public ProsessStreamProcessorBase StdOut { get; internal set; }
         protected Process process;
         ProcessStartInfo psi;
         private readonly bool viaShell;

         public Process Process { get { return process; } }

         public ProcessHelper(String cmd, String args, bool viaShell, String workingdir)
         {
            this.viaShell = viaShell;
            process = new Process();
            psi = process.StartInfo;
            psi.Arguments = args;
            psi.FileName = cmd;
            psi.UseShellExecute = false;
            psi.LoadUserProfile = false;
            psi.WindowStyle = ProcessWindowStyle.Hidden;
            psi.WorkingDirectory = workingdir;

            if (viaShell)
            {
               StringBuilder sb = new StringBuilder(256);
               sb.Append("/c ");
               formatCmd(sb, cmd, args);
               psi.Arguments = sb.ToString();
               psi.FileName = Environment.GetEnvironmentVariable("comspec");
            }
         }

         public void Dispose()
         {
            Utils.FreeAndNil(ref process);
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

            if (StdOut != null) StdOut.Terminate();
            if (StdErr != null) StdErr.Terminate();
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
         public void SetStreams(Object strmOut, object strmErr, bool errorToBoth, Encoding enc)
         {
            this.StdOut = objectToProcessor(strmOut, null, enc);
            this.StdErr = objectToProcessor(strmErr, errorToBoth ? this.StdOut :null, enc);
         }

         private ProsessStreamProcessorBase objectToProcessor (Object x, ProsessStreamProcessorBase slave, Encoding enc)
         {
            if (x == null) return null;

            var strm = x as Stream;
            if (strm != null) return new ProsessStreamProcessor(strm, enc, slave);

            var sb = x as StringBuilder;
            if (sb != null) return new ProsessStringStreamProcessor(sb, enc, slave);

            var lg = x as Logger;
            if (lg == null) throw new BMException("Unexpected object: type [{0}]", x.GetType());

            return new ProsessStreamLogProcessor (lg, enc, slave);
         }


         /// <summary>
         /// Baseclass for handling redirected output
         /// </summary>
         public abstract class ProsessStreamProcessorBase
         {
            public readonly Encoding encoding;
            public readonly ProsessStreamProcessorBase SlaveProcessor;

            protected ProsessStreamProcessorBase(Encoding enc, ProsessStreamProcessorBase slave)
            {
               SlaveProcessor = slave;
               encoding = enc;
            }

            protected void forwardToSlave (object sender, DataReceivedEventArgs e)
            {
               if (SlaveProcessor != null) SlaveProcessor.OnData(sender, e);
            }

            public abstract void OnData(object sender, DataReceivedEventArgs e);
            public abstract void Terminate();

            public abstract String ReadAll();
         }

         public class ProsessStreamLogProcessor: ProsessStreamProcessorBase
         {
            public readonly Logger Logger;

            public ProsessStreamLogProcessor(Logger logger, Encoding enc, ProsessStreamProcessorBase slave)
               : base(enc, slave)
            {
               Logger = logger;
            }

            public override void OnData(object sender, DataReceivedEventArgs e)
            {
               Logger.Log(e.Data);
               forwardToSlave(sender, e);
            }
            public override void Terminate() {}

            public override String ReadAll()
            {
               return null;
            }
         }


         /// <summary>
         /// Processes redirected output by storing the output in a stream.
         /// </summary>
         public class ProsessStreamProcessor : ProsessStreamProcessorBase
         {
            public readonly Stream Strm;
            private byte[] buf;

            public ProsessStreamProcessor(Stream x, Encoding enc, ProsessStreamProcessorBase slave): base(enc, slave)
            {
               Strm = x;
            }

            public override void OnData(object sender, DataReceivedEventArgs e)
            {
               if (e.Data == null) return;
               int len = 2 + 2 * e.Data.Length;
               if (buf == null || buf.Length < len)
                  buf = new byte[len];
               len = encoding.GetBytes(e.Data, 0, e.Data.Length, buf, 0);
               buf[len] = 10; //lf
               Strm.Write(buf, 0, len+1);
               forwardToSlave(sender, e);
            }

            public override String ReadAll()
            {
               Strm.Position = 0;
               var sr = new StreamReader(Strm, encoding);
               return sr.ReadToEnd();
            }

            private void terminateStream (Stream x)
            {
               if (x.CanSeek) x.Position = 0;
               var x2 = x as ProcessPipeStream;
               if (x2 != null)
                  x2.MarkTerminated();
            }

            public override void Terminate()
            {
               terminateStream(Strm);
            }

         }


         /// <summary>
         /// Processes redirected output by storing the output in a StringBuilder
         /// </summary>
         public class ProsessStringStreamProcessor : ProsessStreamProcessorBase
         {
            public readonly StringBuilder Buffer;

            public ProsessStringStreamProcessor(StringBuilder sb, Encoding enc, ProsessStreamProcessorBase slave)
               : base(enc, slave)
            {
               Buffer = sb;
            }

            public override void OnData(object sender, DataReceivedEventArgs e)
            {
               if (e.Data == null) return;
               Buffer.AppendLine(e.Data);
               forwardToSlave(sender, e);
            }

            public override String ReadAll()
            {
               return Buffer.ToString();
            }

            public override void Terminate()
            {
            }
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
            if (String.IsNullOrEmpty(str)) return;
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


      public class ProcessPipeStream : Stream
      {
         private const int MAX_SEM = 10000;
         private class Buffer
         {
            public Buffer Next;
            public byte[] Bytes;
            private int offset;
            public Buffer(byte[] bytes, int offset, int count)
            {
               Bytes = new byte[count];
               Array.Copy(bytes, offset, Bytes, 0, count);
            }

            public int CopyBytes (byte[] b, int dstOffs, int dstlen)
            {
               int possible = Bytes.Length - offset;
               if (possible < dstlen) dstlen = possible;
               if (dstlen>0)
               {
                  Array.Copy(this.Bytes, offset, b, dstOffs, dstlen);
                  offset += dstlen;
               }
               return dstlen;
            }

            public bool EndOfBuffer { get { return offset >= Bytes.Length; } }
         }

         Action<Stream> onClose;

         private Buffer first, last;
         private Object objLock;
         private Semaphore prodSemaphore, consSemaphore;
         private bool eof;
 
         public ProcessPipeStream(Action<Stream> onClose=null)
         {
            this.onClose = onClose;
            objLock = new Object();
            prodSemaphore = new Semaphore(MAX_SEM, MAX_SEM);
            consSemaphore = new Semaphore(0, MAX_SEM);

         }

         public override void Close()
         {
            base.Close();
            if (onClose != null)
            {
               onClose(this);
            }
         }
         
         public override bool CanRead
         {
            get { return true; }
         }

         public override bool CanSeek
         {
            get { return false; }
         }

         public override bool CanWrite
         {
            get { return true; }
         }

         public override void Flush()
         {
         }

         public override long Length
         {
            get { throw new NotImplementedException(); }
         }

         public override long Position
         {
            get
            {
               throw new NotImplementedException();
            }
            set
            {
               throw new NotImplementedException();
            }
         }

         public void MarkTerminated ()
         {
            consSemaphore.Release();
         }

         public override int Read(byte[] buffer, int offset, int count)
         {
            if (count==0 || eof) return 0;
            


            int releaseCount = 0;
            int cnt = 0;

            while (true)
            {
               consSemaphore.WaitOne();
               lock (objLock)
               {
                  if (first == null)
                  {
                     eof = true;
                     goto EXIT_READ;
                  }

                  int read = first.CopyBytes(buffer, offset, count);
                  cnt += read;
                  count -= read;
                  offset += read;
                  if (!first.EndOfBuffer)
                  {
                     consSemaphore.Release(1);
                     goto EXIT_READ; //Not all bytes could be copied
                  }

                  ++releaseCount;
                  if (first == last)
                  {
                     first = null;
                     last = null;
                  }
                  else
                     first = first.Next;
                  if (first == null) break;
               }
            }

            EXIT_READ:
            if (releaseCount > 0) prodSemaphore.Release(releaseCount);
            return cnt;
         }

         public override long Seek(long offset, SeekOrigin origin)
         {
            throw new NotImplementedException();
         }

         public override void SetLength(long value)
         {
            throw new NotImplementedException();
         }

         public override void Write(byte[] buffer, int offset, int count)
         {

            if (count > 0)
            {
               prodSemaphore.WaitOne();
               Buffer buf = new Buffer(buffer, offset, count);
               lock (objLock)
               {
                  if (last == null)
                  {
                     last = first = buf;
                  }
                  else
                  {
                     last = last.Next = buf;
                  }
               }
            }

            consSemaphore.Release();
         }
      }

   }
}