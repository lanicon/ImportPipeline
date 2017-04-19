/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
            gridStatus.Columns[0].DefaultCellStyle.Alignment = DataGridViewContentAlignment.TopLeft;
            gridStatus.Columns[0].AutoSizeMode = DataGridViewAutoSizeColumnMode.AllCells;
            gridStatus.Columns[1].DefaultCellStyle.WrapMode = DataGridViewTriState.True;
            gridStatus.Columns[1].AutoSizeMode = DataGridViewAutoSizeColumnMode.None;
            gridStatus.DefaultCellStyle.SelectionBackColor = gridStatus.DefaultCellStyle.BackColor;
            gridStatus.DefaultCellStyle.SelectionForeColor = gridStatus.DefaultCellStyle.ForeColor;
         }
         catch (Exception ex)
         {
            Logs.ErrorLog.Log(ex);
            throw;
         }
      }

      private Logger importLog = Logs.CreateLogger("import", "importer");
      private Logger errorLog = Logs.CreateLogger("error", "importer");

      private AutoCompleter ac;
      private void Form1_Load(object sender, EventArgs e)
      {
         try
         {
            AppDomain.CurrentDomain.AssemblyResolve += onResolve;
            AppDomain.CurrentDomain.AssemblyLoad += onLoad;
            trySetIcon();
            String dir = Assembly.GetExecutingAssembly().Location;

            StringDict dirs = new StringDict();
            dir = IOUtils.FindDirectoryToRoot(Path.GetDirectoryName(dir), "ImportDirs");
            if (dir != null) dirs.Add(dir, null);

            StringDict files = new StringDict();
            foreach (var f in History.LoadHistory(HISTORY_KEY))
            {
               files[f] = null;
               dir = Path.GetDirectoryName(Path.GetDirectoryName(f));
               if (!String.IsNullOrEmpty(dir))
                  dirs[dir] = null;
            }

            foreach (var kvp in dirs)
            {
               FileTree tree = new FileTree();
               tree.AddFileFilter(@"\\import\.xml$", true);
               tree.ReadFiles(kvp.Key);
               if (tree.Files.Count != 0)
               {
                  tree.Files.Sort();
                  foreach (var relfile in tree.Files) 
                     files[tree.GetFullName(relfile)] = null;
               }
            }

            ac = new DirectoryAutocompleter(comboBox1, files.Select(kvp => kvp.Key).ToList());
            if (comboBox1.Items.Count > 0)
               comboBox1.SelectedIndex = 0;
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

            gridStatus.Rows.Clear();
            gridStatus.Rows.Add(2 + asyncAdmin.Report.DatasourceReports.Count);
            int i = 0;
            if (asyncAdmin.Report.DatasourceReports.Count > 0)
            {
               var line = new LeveledStringBuilder(null, "    ");
               foreach (var rep in asyncAdmin.Report.DatasourceReports)
               {
                  line.Buffer.Clear();
                  addRow(gridStatus.Rows[i++], rep.DatasourceName, rep.ToString(line, false).ToString());
               }
            }
            addRow(gridStatus.Rows[i++], "Unknown switches", asyncAdmin.Report.UnknownSwitches);
            addRow(gridStatus.Rows[i++], "Mentioned switches", asyncAdmin.Report.MentionedSwitches);
            gridStatus.Columns[1].Width = gridStatus.Width - gridStatus.Columns[0].Width - 10;
            Utils.FreeAndNil(ref asyncAdmin);
         }
         catch
         {
            Utils.FreeAndNil(ref asyncAdmin);
            throw;
         }
      }

      private static void addRow (DataGridViewRow row, String k, String v)
      {
         var cells = row.Cells;
         cells[0].Value = k;
         cells[1].Value = v;
      }

      private void import2()
      {
         if (comboBox1.SelectedIndex < 0) throw new BMException("No entry selected.");
         ac.PushSelectedItem (HISTORY_KEY);
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
         asyncAdmin.Start(uiToFlags(), txtSwitches.Text, comboBox1.Text, activeDSses, Invariant.ToInt32(txtMaxRecords.Text, -1), Invariant.ToInt32(txtMaxEmits.Text, -1));
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
         gridStatus.Rows.Clear();
         gridStatus.Rows.Add(1);
         gridStatus.Rows[0].Cells[0].Value = "Running..."; 

         import2();
      }

      private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
      {
         importLog.Log ("selindexchanged");
         dsList.Items.Clear();
         using (ImportEngine engine = new ImportEngine())
         {
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
      }

      private void checkBox1_CheckedChanged(object sender, EventArgs e)
      {
         var cb = (CheckBox)sender;
         //Logs.DebugLog.Log ("checked=" + cb.Checked);
         button1.Visible = cb.Checked;
         textBox1.Visible = cb.Checked;
         textBox2.Visible = cb.Checked;
      }

      private void comboBox1_TextUpdate(object sender, EventArgs e)
      {
         //importLog.Log("text update");
         var cb = sender as ComboBox;
         String fn = cb.Text;
         if (File.Exists (fn))
         {
            fn = Path.GetFullPath(fn);
            foreach (var item in cb.Items)
               if (fn.Equals(item.ToString(), StringComparison.InvariantCultureIgnoreCase)) return;
            int selIndex = cb.Items.Count;
            cb.Items.Add(fn);
            cb.SelectedIndex = selIndex;
         }

      }

      private void lbStatus_SelectedIndexChanged(object sender, EventArgs e)
      {

      }

      private void lbStatus_DrawItem(object sender, DrawItemEventArgs e)
      {
         ListBox lb = (ListBox)sender;

         /*chk if list box has any items*/
         if (e.Index > -1)
         {
            string s = lb.Items[e.Index].ToString();

            /*Normal items*/
            if ((e.State & DrawItemState.Focus) == 0)
            {
               e.Graphics.FillRectangle(
                   new SolidBrush(SystemColors.Window),
                   e.Bounds);
               e.Graphics.DrawString(s, Font,
                   new SolidBrush(SystemColors.WindowText),
                   e.Bounds);
               e.Graphics.DrawRectangle(
                   new Pen(SystemColors.Highlight), e.Bounds);
            }
            else /*Selected item, needs highlighting*/
            {
               e.Graphics.FillRectangle(
                   new SolidBrush(SystemColors.Highlight),
                   e.Bounds);
               e.Graphics.DrawString(s, Font,
                   new SolidBrush(SystemColors.HighlightText),
                   e.Bounds);
            }
         }
      }

      private void lbStatus_MeasureItem(object sender, MeasureItemEventArgs e)
      {
         ListBox lb = (ListBox)sender;
         string s = lb.Items[e.Index].ToString();
         SizeF sf = e.Graphics.MeasureString(s, lb.Font, lb.Width);
         int htex = (e.Index == 0) ? 15 : 10;
         e.ItemHeight = (int)sf.Height + htex;
         e.ItemWidth = Width;
      }

      private void btnOpen_Click(object sender, EventArgs e)
      {
         if (!String.IsNullOrEmpty(comboBox1.Text))
            try
            {
               openFileDialog1.InitialDirectory = Path.GetDirectoryName(Path.GetDirectoryName(comboBox1.Text));
            }
            catch { }
         if (openFileDialog1.ShowDialog() != System.Windows.Forms.DialogResult.OK) return;
         int pos = ac.AddItem(openFileDialog1.FileName);

         comboBox1.SelectedIndex = pos;
         ac.PushSelectedItem();
      }
   }

   class DirectoryAutocompleter: AutoCompleter
   {
      public DirectoryAutocompleter(ComboBox cb, List<String> list)
         : base(cb, list, AutoCompleter.DirNameConverter)
      {
      }

      protected override void Push(List<Autocompleter_Elt> list, int ix)
      {
         Autocompleter_Elt x = list[ix];
         String argDir = Path.GetDirectoryName(x.ToString());

         int j;
         for (j = 0; j < list.Count; j++)
         {
            if (argDir == Path.GetDirectoryName(list[j].ToString())) break;
         }

         if (j < ix)
         {
            var tmp = list[j];
            list[j] = list[ix];
            list[ix] = tmp;
            ix = j;
         }
         base.Push(list, ix);
      }

   }
}
