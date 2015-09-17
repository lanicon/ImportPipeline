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

namespace TestPostProcessor
{
   partial class Form1
   {
      /// <summary>
      /// Required designer variable.
      /// </summary>
      private System.ComponentModel.IContainer components = null;

      /// <summary>
      /// Clean up any resources being used.
      /// </summary>
      /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
      protected override void Dispose(bool disposing)
      {
         if (disposing && (components != null))
         {
            components.Dispose();
         }
         base.Dispose(disposing);
      }

      #region Windows Form Designer generated code

      /// <summary>
      /// Required method for Designer support - do not modify
      /// the contents of this method with the code editor.
      /// </summary>
      private void InitializeComponent()
      {
         this.button1 = new System.Windows.Forms.Button();
         this.label1 = new System.Windows.Forms.Label();
         this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
         this.listBox1 = new System.Windows.Forms.ListBox();
         this.checkBox1 = new System.Windows.Forms.CheckBox();
         this.SuspendLayout();
         // 
         // button1
         // 
         this.button1.Location = new System.Drawing.Point(53, 46);
         this.button1.Name = "button1";
         this.button1.Size = new System.Drawing.Size(75, 23);
         this.button1.TabIndex = 0;
         this.button1.Text = "Open";
         this.button1.UseVisualStyleBackColor = true;
         this.button1.Click += new System.EventHandler(this.button1_Click);
         // 
         // label1
         // 
         this.label1.AutoSize = true;
         this.label1.Location = new System.Drawing.Point(238, 49);
         this.label1.Name = "label1";
         this.label1.Size = new System.Drawing.Size(35, 13);
         this.label1.TabIndex = 1;
         this.label1.Text = "label1";
         // 
         // openFileDialog1
         // 
         this.openFileDialog1.FileName = "openFileDialog1";
         this.openFileDialog1.Filter = "*.txt|*.txt|*.csv|*.csv";
         // 
         // listBox1
         // 
         this.listBox1.FormattingEnabled = true;
         this.listBox1.HorizontalScrollbar = true;
         this.listBox1.Location = new System.Drawing.Point(160, 81);
         this.listBox1.Name = "listBox1";
         this.listBox1.ScrollAlwaysVisible = true;
         this.listBox1.Size = new System.Drawing.Size(698, 264);
         this.listBox1.TabIndex = 2;
         // 
         // checkBox1
         // 
         this.checkBox1.AutoSize = true;
         this.checkBox1.Location = new System.Drawing.Point(48, 81);
         this.checkBox1.Name = "checkBox1";
         this.checkBox1.Size = new System.Drawing.Size(77, 17);
         this.checkBox1.TabIndex = 3;
         this.checkBox1.Text = "case sens.";
         this.checkBox1.UseVisualStyleBackColor = true;
         // 
         // Form1
         // 
         this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
         this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
         this.ClientSize = new System.Drawing.Size(893, 430);
         this.Controls.Add(this.checkBox1);
         this.Controls.Add(this.listBox1);
         this.Controls.Add(this.label1);
         this.Controls.Add(this.button1);
         this.Name = "Form1";
         this.Text = "Form1";
         this.Load += new System.EventHandler(this.Form1_Load);
         this.ResumeLayout(false);
         this.PerformLayout();

      }

      #endregion

      private System.Windows.Forms.Button button1;
      private System.Windows.Forms.Label label1;
      private System.Windows.Forms.OpenFileDialog openFileDialog1;
      private System.Windows.Forms.ListBox listBox1;
      private System.Windows.Forms.CheckBox checkBox1;
   }
}

