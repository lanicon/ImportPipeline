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

namespace Bitmanager.Importer
{
   public partial class Form1 : Form
   {
      private const String HISTORY_KEY = @"Software\Bitmanager\ImportPipeline";
      public Form1()
      {
         InitializeComponent();
         Bitmanager.Core.GlobalExceptionHandler.HookGlobalExceptionHandler();
      }

      private void Form1_Load(object sender, EventArgs e)
      {
         String dir = Assembly.GetExecutingAssembly().Location;
         dir = IOUtils.FindDirectoryToRoot(Path.GetDirectoryName(dir), "ImportDirs");
         if (dir!=null)
         {
            FileTree tree = new FileTree ();
            tree.AddFileFilter (@"\\import\.xml$", true);
            tree.ReadFiles (dir);
            if (tree.Files.Count != 0)
            {
               History.LoadHistory(comboBox1, HISTORY_KEY, tree.Files.Select(f=>tree.GetFullName(f)).ToList());
               return;
            }
         }
         History.LoadHistory(comboBox1, HISTORY_KEY);
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
      private void button1_Click(object sender, EventArgs e)
      {
         import();
      }

      private void button2_Click(object sender, EventArgs e)
      {
      }

      private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
      {
         dsList.Items.Clear();
         ImportEngine engine = new ImportEngine();
         engine.Load(comboBox1.Text);
         uiFromFlags (engine);

         foreach (var ds in engine.Datasources)
         {
            dsList.Items.Add (ds.Name, ds.Active);
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
      private void uiToFlags(ImportEngine eng)
      {
         eng.ImportFlags = 0;
         foreach (var c in grpFlags.Controls)
         {
            CheckBox cb = c as CheckBox;
            if (cb == null) continue;
            _ImportFlags flag = Invariant.ToEnum<_ImportFlags>(cb.Text);
            if (cb.Checked) eng.ImportFlags |= flag;
         }
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
   }

   //public class Cursor : IDisposable
   //{
   //   public Cursor()
   //   {
   //   }


   //   public void Dispose()
   //   {
   //      throw new NotImplementedException();
   //   }
   //}
}
