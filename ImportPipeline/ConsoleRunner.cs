using Bitmanager.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Bitmanager.Java
{
   public class ConsoleRunner: IDisposable
   {
      protected Logger consoleLogger, consoleErrLogger, logger;
      public readonly ProcessHostSettings Settings;
      public readonly String Name;
      public Process process;
      protected int remainingRestarts;
      protected int exitCode;
      protected bool errorsDuringExit;

      public ConsoleRunner(ProcessHostSettings settings, String name)
      {
         Settings = settings;
         String from = String.Format(settings.LogFrom, name);
         logger = Logs.CreateLogger(settings.LogName, from);
         optClearLog(logger);

         remainingRestarts = settings.MaxRestarts;
         if (remainingRestarts < 0)
            remainingRestarts = int.MaxValue;
         else if (remainingRestarts == 0) remainingRestarts = 1;

         logger.Log("Environment variables:");
         foreach (DictionaryEntry kvp in Environment.GetEnvironmentVariables())
         {
            logger.Log("-- {0}: {1}", kvp.Key, kvp.Value);
         }

         consoleLogger = Logs.CreateLogger(settings.LogName, from);
         consoleErrLogger = Logs.CreateLogger(settings.ErrorLogName, from);
         consoleErrLogger.LogType = _LogType.ltError;

         optClearLog(consoleLogger);
         optClearLog(consoleErrLogger);

         if (!settings.LogName.Equals(settings.ErrorLogName, StringComparison.InvariantCultureIgnoreCase))
            consoleErrLogger = new MultiLogger(consoleLogger, consoleErrLogger);
      }

      protected void optClearLog(Logger x)
      {
         if (!Settings.ClearLogs) return;
         x.Clear();
         x.Log("-- CLEARED -- (clearlogs=true [default])");
      }

      protected String getExeName()
      {
         return expandAllEnvironmentVariables(Settings.ExeName);
      }
      protected String getArguments()
      {
         return expandAllEnvironmentVariables(Settings.Arguments);
      }

      protected static String expandAllEnvironmentVariables(String x)
      {
         String repl = x;
         while (true)
         {
            String tmp = Environment.ExpandEnvironmentVariables(repl);
            if (tmp.Length == repl.Length && tmp == repl) return repl;
            repl = tmp;
         }
      }

      public virtual void Start()
      {

         errorsDuringExit = false;
         logger.Log();
         Process p = new Process();
         ProcessStartInfo psi = p.StartInfo;
         psi.Arguments = getArguments();
         psi.FileName = getExeName();
         psi.UseShellExecute = false;
         psi.RedirectStandardOutput = true;
         psi.RedirectStandardError = true;
         psi.LoadUserProfile = false;
         psi.WindowStyle = ProcessWindowStyle.Normal; //.Hidden;

         logger.Log();
         logger.Log("Starting java process..." + Environment.StackTrace);
         logger.Log("-- process=" + psi.FileName);
         logger.Log("-- arguments=" + psi.Arguments);
         if (Settings.StartDelay >= 0)
         {
            logger.Log("Delaying start with {0}ms", Settings.StartDelay);
            Thread.Sleep(Settings.StartDelay);
         }

         p.OutputDataReceived += OnDataReceived;
         p.ErrorDataReceived += OnErrorReceived;
         p.Start();
         p.BeginOutputReadLine();
         p.BeginErrorReadLine();
         process = p;
         logger.Log("-- process={0}", process.Id);
      }

      public virtual bool WaitForExit(int ms)
      {
         if (checkExited()) return true;
         logger.Log("Waiting {0} ms for exit...", ms);
         if (process.WaitForExit(ms))
            return checkExited();
         return false;
      }

      protected bool checkExited()
      {
         if (process == null) return true;
         if (!process.HasExited) return false;
         exitCode = process.ExitCode;
         if ((uint)exitCode == 0xC000013A && this.Settings.ShutdownUrl == null)
            exitCode = 0;

         _LogType lt = _LogType.ltInfo;
         if (exitCode != 0)
         {
            lt = _LogType.ltError;
            errorsDuringExit = true;
         }
         Utils.FreeAndNil(ref process);
         logger.Log(lt, "Process exited with exitcode={0} (0x{0:X}).", exitCode);
         // logger.Log(lt, "-- msg=" + Marshal.GetExceptionForHR (exitCode & 0xFFFF).Message);
         return true;
      }

      public virtual bool Stop_Initiate()
      {
         if (checkExited()) return false;
         if (Settings.ShutdownUrl == null) return false;

         logger.Log("Sending shutdownUrl {0}, method={1}", Settings.ShutdownUrl, Settings.ShutdownMethod);
         Uri url = new Uri(Settings.ShutdownUrl);
         Exception saved = null;
         using (WebClient client = new WebClient())
         {
            try 
            {
               client.DownloadData(url);
               return true;  //wait  some time before the ctrl-c
            }
            catch (Exception err)
            {
               saved = err;
            }
         }
         logger.Log("Shutdown failed...");
         logger.Log(saved);
         return false; //No wait needed
      }

      [DllImport("user32.dll", CharSet = CharSet.Auto)]
      static extern IntPtr SendMessage(IntPtr hWnd, UInt32 Msg, IntPtr wParam, IntPtr lParam);
      const int WM_CHAR = 0x102;
      const int WM_CLOSE = 0x10;

      public void CloseWindow(IntPtr hWindow)
      {
         SendMessage(hWindow, WM_CLOSE, IntPtr.Zero, IntPtr.Zero);
      }
      
      public virtual bool Stop_CtrlC()
      {
         if (checkExited()) return false;

         IntPtr hwnd = process.MainWindowHandle;
         if (hwnd.ToInt64() != 0)
         {
            //   IntPtr x = SendMessage(hwnd, WM_CHAR, new IntPtr(0x3), new IntPtr (1));
            //   logger.Log(_LogType.ltInfo, "SendMessage rc={0}", x);
            //   return true;
            IntPtr x = SendMessage(hwnd, WM_CLOSE, IntPtr.Zero, IntPtr.Zero);
            logger.Log(_LogType.ltInfo, "SendMessage rc={0}", x);
            return true;
         }

         if (ConsoleHelpers.GenerateConsoleCtrlEvent(CtrlTypes.CTRL_C_EVENT, 0)) //Send CTRL_C to ourselves (we own the process group)
            logger.Log(_LogType.ltInfo, "Successfull sent CRTL_C...");
         else
         {
            int lastErr = Marshal.GetLastWin32Error();
            Exception err = Marshal.GetExceptionForHR (Marshal.GetHRForLastWin32Error());
            logger.Log(_LogType.ltError, "Sending CRTL_C failed: " + lastErr.ToString() + "; " + err.Message);
         }
         return true;
      }
      public virtual bool Stop_Kill()
      {
         if (checkExited()) return false;

         logger.Log(_LogType.ltError, "-- Killing process...");
         process.Kill();
         errorsDuringExit = true;
         return true;
      }
      public bool CheckStoppedAndDispose()
      {
         if (checkExited()) return !errorsDuringExit;

         Utils.FreeAndNil(ref process);
         return false;
      }

      private void OnDataReceived(object Sender, DataReceivedEventArgs e)
      {
         if (e.Data == null) return;
         consoleLogger.Log(e.Data);
      }
      private void OnErrorReceived(object Sender, DataReceivedEventArgs e)
      {
         if (e.Data == null) return;

         consoleErrLogger.Log(e.Data);
      }


      public void Dispose()
      {
         Utils.FreeAndNil(ref process);
      }
   }
}
