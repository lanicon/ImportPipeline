using Bitmanager.ImportPipeline;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Bitmanager.Importer
{
   public partial class Form1 : Form
   {
      public Form1()
      {
         InitializeComponent();
      }

      private void button1_Click(object sender, EventArgs e)
      {
         if (openFileDialog1.ShowDialog() != System.Windows.Forms.DialogResult.OK) return;

         ImportEngine engine = new ImportEngine();
         engine.Load(openFileDialog1.FileName);
         engine.Import();
      }

      private void Form1_Load(object sender, EventArgs e)
      {
         Bitmanager.Core.GlobalExceptionHandler.HookGlobalExceptionHandler();
      }
   }
}
