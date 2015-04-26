using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Bitmanager.Core;
using Bitmanager.IO;
using System.Reflection;
using System.IO;
using Bitmanager.ImportPipeline;
using System.Threading;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Xml;
using Bitmanager.Xml;

namespace Bitmanager.Importer
{
   public partial class Form1 : Form
   {
      private const int WM_SETICON = 0x80;
      private const int ICON_SMALL = 0;
      private const int ICON_BIG = 1;
      [DllImport("user32.dll")]
      public static extern int SendMessage(IntPtr hwnd, int message, int wParam, IntPtr lParam);


      private const String HISTORY_KEY = @"Software\Bitmanager\ImportPipeline";
      public Form1()
      {
         try
         {
            Bitmanager.Core.GlobalExceptionHandler.HookGlobalExceptionHandler();
            InitializeComponent();
         }
         catch (Exception ex)
         {
            Logs.ErrorLog.Log(ex);
            throw;
         }
      }

      private Logger importLog = Logs.CreateLogger("import", "importer");
      private Logger errorLog = Logs.CreateLogger("error", "importer");
      private void Form1_Load(object sender, EventArgs e)
      {
         try
         {
            AppDomain.CurrentDomain.AssemblyResolve += onResolve;
            AppDomain.CurrentDomain.AssemblyLoad += onLoad;
            trySetIcon();
            String dir = Assembly.GetExecutingAssembly().Location;

            dir = IOUtils.FindDirectoryToRoot(Path.GetDirectoryName(dir), "ImportDirs");
            if (dir != null)
            {
               FileTree tree = new FileTree();
               tree.AddFileFilter(@"\\import\.xml$", true);
               tree.ReadFiles(dir);
               if (tree.Files.Count != 0)
               {
                  History.LoadHistory(comboBox1, HISTORY_KEY, tree.Files.Select(f => tree.GetFullName(f)).ToList());
                  return;
               }
            }
            History.LoadHistory(comboBox1, HISTORY_KEY);
         }
         catch (Exception ex)
         {
            Logs.ErrorLog.Log(ex);
            throw;
         }
      }

      void onLoad(object sender, AssemblyLoadEventArgs args)
      {
         errorLog.Log("onLoad: '{0}'", args.LoadedAssembly);
      }

      Assembly onResolve(object sender, ResolveEventArgs args)
      {
         errorLog.Log("onResolve: '{0}'", args.Name);
         errorLog.Log("-- requested by {0}", args.RequestingAssembly);
         return null;
      }
      private void trySetIcon()
      {
         try
         {
            Icon icon = Icon.ExtractAssociatedIcon(Assembly.GetExecutingAssembly().Location);
            if (icon == null) return;
            this.Icon = icon;
            SendMessage(this.Handle, WM_SETICON, ICON_SMALL, icon.Handle);
            SendMessage(this.Handle, WM_SETICON, ICON_BIG, icon.Handle);
         }
         catch (Exception e)
         {
            Logs.ErrorLog.Log(e);
         }
      }

      private void import()
      {
         if (comboBox1.SelectedIndex < 0) return;

         Cursor.Current = Cursors.WaitCursor;
         UseWaitCursor = true;
         Application.DoEvents();
         try
         {
            History.SaveHistory(comboBox1, HISTORY_KEY);

            String settingsFile = comboBox1.Text;
            ImportEngine engine = new ImportEngine();
            engine.Load(settingsFile);
            uiToFlags(engine);

            String[] activeDSses = null;
            var items = dsList.Items;
            if (items.Count > 0)
            {
               var list = new List<String>();
               for (int i = 0; i < items.Count; i++)
               {
                  if (!dsList.GetItemChecked(i)) continue;
                  list.Add((String)items[i]);
               }
               activeDSses = list.ToArray();
            }
            engine.Import(activeDSses);
         }
         finally
         {
            UseWaitCursor = false;
         }
      }

      private AsyncAdmin asyncAdmin;
      private void btnCancel_Click(object sender, EventArgs e)
      {
         timer1.Enabled = false;
         if (asyncAdmin != null)
         {
            asyncAdmin.Cancel();
            Utils.FreeAndNil(ref asyncAdmin);
         }
         UseWaitCursor = false;
         enableAllButCancel();
      }

      private void tryJson()
      {
         JObject x = new JObject();
         x.Add("date", DateTime.UtcNow);
         x.Add("double", 123.45);
         MemoryStream m = new MemoryStream();
         JsonWriter wtr = new JsonTextWriter(new StreamWriter(m));
         x.WriteTo(wtr);
         wtr.Flush();
         String result = Encoding.UTF8.GetString(m.GetBuffer());
         Logs.ErrorLog.Log(result);
      }
      private void timer1_Tick(object sender, EventArgs e)
      {
         if (asyncAdmin == null || !asyncAdmin.CheckStopped()) return;
         try
         {
            timer1.Enabled = false;
            UseWaitCursor = false;
            enableAllButCancel();
            asyncAdmin.Stop();
            //lblStatus.Text = asyncAdmin.Report.ErrorMessage;
            //if (lblStatus.Text != null) lblStatus.Text = lblStatus.Text.Replace('\r', ' ').Replace('\n', ' ');

            lbStatus.Items.Clear();
            lbStatus.Items.Add(lblStatus.Text);
            foreach (var rep in asyncAdmin.Report.DatasourceReports)
            {
               lbStatus.Items.Add(rep.ToString());
            }
            Utils.FreeAndNil(ref asyncAdmin);
         }
         catch
         {
            Utils.FreeAndNil(ref asyncAdmin);
            throw;
         }
      }

      private void import2()
      {
         if (comboBox1.SelectedIndex < 0) return;

         History.SaveHistory(comboBox1, HISTORY_KEY);
         String[] activeDSses = null;
         var items = dsList.Items;
         if (items.Count > 0)
         {
            var list = new List<String>();
            for (int i = 0; i < items.Count; i++)
            {
               if (!dsList.GetItemChecked(i)) continue;
               list.Add((String)items[i]);
            }
            activeDSses = list.ToArray();
         }

         AsyncAdmin asyncAdmin = new AsyncAdmin();
         asyncAdmin.Start(uiToFlags(), comboBox1.Text, activeDSses, Invariant.ToInt32(txtMaxRecords.Text, -1), Invariant.ToInt32(txtMaxEmits.Text, -1));
         this.asyncAdmin = asyncAdmin;

         timer1.Enabled = true;
         Cursor.Current = Cursors.WaitCursor;
         UseWaitCursor = true;
         disableAllButCancel();
      }

      private void enableAllButCancel()
      {
         foreach (var c in Controls)
         {
            Control control = c as Control;
            if (control == null) continue;
            if (control == btnCancel) continue;
            control.Enabled = true;
         }
         btnCancel.Enabled = false;
      }
      private void disableAllButCancel()
      {
         foreach (var c in this.Controls)
         {
            Control control = c as Control;
            if (control == null) continue;
            Logs.DebugLog.Log("ctrl={0}", control.Name);
            if (control == btnCancel) continue;
            Logs.DebugLog.Log("-- disbled");
            control.Enabled = false;
         }
         btnCancel.Enabled = true;
      }
      private void button1_Click(object sender, EventArgs e)
      {
         lblStatus.Text = null;
         lbStatus.Items.Clear();
         lbStatus.Items.Add("Running...");

         import2();
      }

      private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
      {
         dsList.Items.Clear();
         ImportEngine engine = new ImportEngine();
         engine.Load(comboBox1.Text);
         cbEndpoints.Items.Clear();
         cbEndpoints.Items.Add(String.Empty);
         foreach (var item in engine.Endpoints) cbEndpoints.Items.Add(item.Name);


         cbPipeLines.Items.Clear();
         cbPipeLines.Items.Add(String.Empty);
         foreach (var item in engine.Pipelines) cbPipeLines.Items.Add(item.Name);

         
         uiFromFlags(engine);
         txtMaxRecords.Text = engine.MaxAdds.ToString();
         txtMaxEmits.Text = engine.MaxEmits.ToString();

         foreach (var ds in engine.Datasources)
         {
            dsList.Items.Add(ds.Name, ds.Active);
         }
      }

      private void uiFromFlags(ImportEngine eng)
      {
         foreach (var c in grpFlags.Controls)
         {
            CheckBox cb = c as CheckBox;
            if (cb == null) continue;
            _ImportFlags flag = Invariant.ToEnum<_ImportFlags>(cb.Text);
            cb.Checked = (eng.ImportFlags & flag) != 0;
         }
      }
      private _ImportFlags uiToFlags()
      {
         _ImportFlags flags = 0;
         foreach (var c in grpFlags.Controls)
         {
            CheckBox cb = c as CheckBox;
            if (cb == null || !cb.Checked) continue;
            flags |= Invariant.ToEnum<_ImportFlags>(cb.Text);
         }
         return flags;
      }

      private void uiToFlags(ImportEngine eng)
      {
         eng.ImportFlags = uiToFlags();
      }


      private void button3_Click(object sender, EventArgs e)
      {
         comboBox1_SelectedIndexChanged(comboBox1, e);
      }

      private void button4_Click(object sender, EventArgs e)
      {
         String dir = Path.GetDirectoryName(comboBox1.Text);
         if (String.IsNullOrEmpty(dir)) return;
         System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo()
         {
            FileName = dir,
            UseShellExecute = true,
            Verb = "open"
         });
      }

      private void button5_Click(object sender, EventArgs e)
      {
         String dir = Path.GetDirectoryName(comboBox1.Text);
         if (String.IsNullOrEmpty(dir)) return;
         System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo()
         {
            FileName = comboBox1.Text,
            UseShellExecute = true,
            Verb = "open"
         });

      }

      private void button6_Click(object sender, EventArgs e)
      {
         if (openFileDialog1.ShowDialog() != System.Windows.Forms.DialogResult.OK) return;

         int idx = comboBox1.Items.Count;
         comboBox1.Items.Add(openFileDialog1.FileName);
         comboBox1.SelectedIndex = idx;
      }

      private void button1_Click_1(object sender, EventArgs e)
      {
         String str = this.textBox1.Text;
         String fmt = this.textBox2.Text.TrimToNull();
         if (fmt != null)
            DateTime.ParseExact(str, fmt, Invariant.Culture);
         else
         {
            XmlHelper hlp = new XmlHelper();
            hlp.LoadXml("<root><converter name='date' /></root>");
            var dc = new ToDateConverter(hlp.SelectMandatoryNode("converter"), "date");
            dc.ConvertScalar(null, str);
         }
         //const String date = "Fri, 26 Nov 1999 10:24:41 +0200";
         //const String date2 = "Thu Apr 26 11:38:36 +0200 2012";
         //const String date3 = "Fri Sep 08 11:09:29 CEST 2006";
         //const String date4 = "Mon, 9 Jan 2012 09:29:39 +0000";
         //var pr = NodaTime.Text.OffsetDateTimePattern.Rfc3339Pattern.Parse("Fri, 26 Nov 1999 10:24:41 +0200");
         //pr = NodaTime.Text.OffsetDateTimePattern.GeneralIsoPattern.Parse("Fri, 26 Nov 1999 10:24:41 +0200");
         //pr = NodaTime.Text.OffsetDateTimePattern.ExtendedIsoPattern.Parse("Fri, 26 Nov 1999 10:24:41 +0200");
         //DateTime pr2;
         ////pr2 = DateTime.ParseExact(date2, "ddd MMM dd HH:mm:ss zzz yyyy", Invariant.Culture);
         ////pr2 = DateTime.ParseExact(date3, "ddd MMM dd HH:mm:ss zzz  yyyy", Invariant.Culture, System.Globalization.DateTimeStyles.AllowWhiteSpaces);


         //   var logger = Logs.DebugLog;
         //foreach (TimeZoneInfo timeZone in TimeZoneInfo.GetSystemTimeZones())
         //{
         //   bool hasDST = timeZone.SupportsDaylightSavingTime;
         //   TimeSpan offsetFromUtc = timeZone.BaseUtcOffset;
         //   TimeZoneInfo.AdjustmentRule[] adjustRules;
         //   string offsetString;

         //   logger.Log("ID: {0}", timeZone.Id);
         //   logger.Log("   Display Name: {0, 40}", timeZone.DisplayName);
         //   logger.Log("   Standard Name: {0, 39}", timeZone.StandardName);
         //   //sw.Write("   Daylight Name: {0, 39}", timeZone.DaylightName);
         //   //sw.Write(hasDST ? "   ***Has " : "   ***Does Not Have ");
         //   //sw.WriteLine("Daylight Saving Time***");
         //   offsetString = String.Format("{0} hours, {1} minutes", offsetFromUtc.Hours, offsetFromUtc.Minutes);
         //   logger.Log("   Offset from UTC: {0, 40}", offsetString);
         //}

         //tryJson(); return; //PW moet weg

      }

      private void checkBox1_CheckedChanged(object sender, EventArgs e)
      {
         var cb = (CheckBox)sender;
         //Logs.DebugLog.Log ("checked=" + cb.Checked);
         button1.Visible = cb.Checked;
         textBox1.Visible = cb.Checked;
         textBox2.Visible = cb.Checked;
      }

      private void cbEndpoints_SelectedValueChanged(object sender, EventArgs e)
      {
         //ComboBox cb = (ComboBox)sender;
         //if (cb.SelectedItem > 0)
      }
   }
}
