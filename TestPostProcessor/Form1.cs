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

using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TestPostProcessor
{
   public partial class Form1 : Form
   {
      public Form1()
      {
         InitializeComponent();
         Bitmanager.Core.GlobalExceptionHandler.HookGlobalExceptionHandler();
      }

      List<String> lines;
      private void button1_Click(object sender, EventArgs e)
      {
         if (openFileDialog1.ShowDialog() != System.Windows.Forms.DialogResult.OK) return;

         using (var rdr = File.OpenText(openFileDialog1.FileName))
         {
            lines = new List<string>();
            while (true)
            {
               String line = rdr.ReadLine();
               if (line == null) break;
               lines.Add(line);
            }
         }
         diagnose();

      }

      private void addMsg (String msg, params Object[] args)
      {
         listBox1.Items.Add(String.Format(msg, args));
      }
      private void diagnose ()
      {
         StringComparer cmp;
         bool caseSens = checkBox1.Checked;
         cmp = caseSens ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase;
         listBox1.Items.Clear();
         var dict = new StringDict<int>(!caseSens);
         int cntPerItem = -1;

         for (int i=0; i<lines.Count; )
         {
            String line = lines[i];
            int existing;
            if (dict.TryGetValue(line, out existing))
               addMsg("Line {0} clashes with existing line {1}.", i, existing);
            else
               dict.Add(line, i);
            int start = i;
            for (i++; i<lines.Count; i++)
            {
               String s = lines[i];
               if (!cmp.Equals (s, line)) break;
            }
            int cnt = i-start;
            if (cntPerItem < 0)
               cntPerItem = cnt;
            else if (cnt != cntPerItem)
               addMsg("Line {0}: unexpected count {1}, existing={2}.", start, cnt, cntPerItem);
         }

         addMsg("Lines={0}, unique={1}, perUnique={2}. ", lines.Count, dict.Count, lines.Count / (double)dict.Count);
      }

      private void Form1_Load(object sender, EventArgs e)
      {

      }
   }
}
