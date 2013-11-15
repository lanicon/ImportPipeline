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
         History.SaveHistory(comboBox1, HISTORY_KEY);

         String settingsFile = comboBox1.Text;
         ImportEngine engine = new ImportEngine();
         engine.Load(settingsFile);
         engine.Import();
      }
      private void button1_Click(object sender, EventArgs e)
      {
         import();
      }

      private void button2_Click(object sender, EventArgs e)
      {
         if (openFileDialog1.ShowDialog() != System.Windows.Forms.DialogResult.OK) return;

         int idx = comboBox1.Items.Count;
         comboBox1.Items.Add(openFileDialog1.FileName);
         comboBox1.SelectedIndex = idx;
         import();
      }
   }
}
