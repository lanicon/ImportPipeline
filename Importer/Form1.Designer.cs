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

namespace Bitmanager.Importer
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
         this.components = new System.ComponentModel.Container();
         this.comboBox1 = new System.Windows.Forms.ComboBox();
         this.btnImport = new System.Windows.Forms.Button();
         this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
         this.dsList = new System.Windows.Forms.CheckedListBox();
         this.label1 = new System.Windows.Forms.Label();
         this.label2 = new System.Windows.Forms.Label();
         this.grpFlags = new System.Windows.Forms.GroupBox();
         this.checkBox3 = new System.Windows.Forms.CheckBox();
         this.checkBox5 = new System.Windows.Forms.CheckBox();
         this.checkBox4 = new System.Windows.Forms.CheckBox();
         this.checkBox2 = new System.Windows.Forms.CheckBox();
         this.cbRetryErrors = new System.Windows.Forms.CheckBox();
         this.cbIgnoreErrors = new System.Windows.Forms.CheckBox();
         this.cbIgnoreLimited = new System.Windows.Forms.CheckBox();
         this.cbTraceValues = new System.Windows.Forms.CheckBox();
         this.cbDoNotRename = new System.Windows.Forms.CheckBox();
         this.CbFullImport = new System.Windows.Forms.CheckBox();
         this.button3 = new System.Windows.Forms.Button();
         this.button4 = new System.Windows.Forms.Button();
         this.button5 = new System.Windows.Forms.Button();
         this.timer1 = new System.Windows.Forms.Timer(this.components);
         this.btnCancel = new System.Windows.Forms.Button();
         this.label3 = new System.Windows.Forms.Label();
         this.txtMaxRecords = new System.Windows.Forms.TextBox();
         this.button1 = new System.Windows.Forms.Button();
         this.textBox1 = new System.Windows.Forms.TextBox();
         this.textBox2 = new System.Windows.Forms.TextBox();
         this.checkBox1 = new System.Windows.Forms.CheckBox();
         this.label4 = new System.Windows.Forms.Label();
         this.txtMaxEmits = new System.Windows.Forms.TextBox();
         this.label6 = new System.Windows.Forms.Label();
         this.label7 = new System.Windows.Forms.Label();
         this.cbEndpoints = new System.Windows.Forms.ComboBox();
         this.cbPipeLines = new System.Windows.Forms.ComboBox();
         this.gridStatus = new System.Windows.Forms.DataGridView();
         this.Datasource = new System.Windows.Forms.DataGridViewTextBoxColumn();
         this.Message = new System.Windows.Forms.DataGridViewTextBoxColumn();
         this.txtSwitches = new System.Windows.Forms.TextBox();
         this.label5 = new System.Windows.Forms.Label();
         this.btnOpen = new System.Windows.Forms.Button();
         this.grpFlags.SuspendLayout();
         ((System.ComponentModel.ISupportInitialize)(this.gridStatus)).BeginInit();
         this.SuspendLayout();
         // 
         // comboBox1
         // 
         this.comboBox1.FormattingEnabled = true;
         this.comboBox1.Location = new System.Drawing.Point(30, 51);
         this.comboBox1.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
         this.comboBox1.Name = "comboBox1";
         this.comboBox1.Size = new System.Drawing.Size(587, 23);
         this.comboBox1.TabIndex = 0;
         this.comboBox1.SelectedIndexChanged += new System.EventHandler(this.comboBox1_SelectedIndexChanged);
         // 
         // btnImport
         // 
         this.btnImport.Location = new System.Drawing.Point(29, 82);
         this.btnImport.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
         this.btnImport.Name = "btnImport";
         this.btnImport.Size = new System.Drawing.Size(87, 26);
         this.btnImport.TabIndex = 1;
         this.btnImport.Text = "Import";
         this.btnImport.UseVisualStyleBackColor = true;
         this.btnImport.Click += new System.EventHandler(this.button1_Click);
         // 
         // openFileDialog1
         // 
         this.openFileDialog1.FileName = "import.xml";
         this.openFileDialog1.Filter = "*.Xml|*.xml";
         this.openFileDialog1.Title = "Open import xml";
         // 
         // dsList
         // 
         this.dsList.FormattingEnabled = true;
         this.dsList.Location = new System.Drawing.Point(674, 51);
         this.dsList.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
         this.dsList.Name = "dsList";
         this.dsList.Size = new System.Drawing.Size(450, 292);
         this.dsList.TabIndex = 3;
         // 
         // label1
         // 
         this.label1.AutoSize = true;
         this.label1.Location = new System.Drawing.Point(675, 28);
         this.label1.Name = "label1";
         this.label1.Size = new System.Drawing.Size(65, 15);
         this.label1.TabIndex = 4;
         this.label1.Text = "Datsources";
         // 
         // label2
         // 
         this.label2.AutoSize = true;
         this.label2.Location = new System.Drawing.Point(27, 28);
         this.label2.Name = "label2";
         this.label2.Size = new System.Drawing.Size(70, 15);
         this.label2.TabIndex = 5;
         this.label2.Text = "Import XML";
         // 
         // grpFlags
         // 
         this.grpFlags.Controls.Add(this.checkBox3);
         this.grpFlags.Controls.Add(this.checkBox5);
         this.grpFlags.Controls.Add(this.checkBox4);
         this.grpFlags.Controls.Add(this.checkBox2);
         this.grpFlags.Controls.Add(this.cbRetryErrors);
         this.grpFlags.Controls.Add(this.cbIgnoreErrors);
         this.grpFlags.Controls.Add(this.cbIgnoreLimited);
         this.grpFlags.Controls.Add(this.cbTraceValues);
         this.grpFlags.Controls.Add(this.cbDoNotRename);
         this.grpFlags.Controls.Add(this.CbFullImport);
         this.grpFlags.Location = new System.Drawing.Point(30, 126);
         this.grpFlags.Name = "grpFlags";
         this.grpFlags.Size = new System.Drawing.Size(165, 288);
         this.grpFlags.TabIndex = 7;
         this.grpFlags.TabStop = false;
         this.grpFlags.Text = "Flags";
         // 
         // checkBox3
         // 
         this.checkBox3.AutoSize = true;
         this.checkBox3.Location = new System.Drawing.Point(16, 252);
         this.checkBox3.Name = "checkBox3";
         this.checkBox3.Size = new System.Drawing.Size(100, 19);
         this.checkBox3.TabIndex = 20;
         this.checkBox3.Text = "NoMailReport";
         this.checkBox3.UseVisualStyleBackColor = true;
         // 
         // checkBox5
         // 
         this.checkBox5.AutoSize = true;
         this.checkBox5.Location = new System.Drawing.Point(17, 227);
         this.checkBox5.Name = "checkBox5";
         this.checkBox5.Size = new System.Drawing.Size(75, 19);
         this.checkBox5.TabIndex = 19;
         this.checkBox5.Text = "LogEmits";
         this.checkBox5.UseVisualStyleBackColor = true;
         // 
         // checkBox4
         // 
         this.checkBox4.AutoSize = true;
         this.checkBox4.Location = new System.Drawing.Point(16, 200);
         this.checkBox4.Name = "checkBox4";
         this.checkBox4.Size = new System.Drawing.Size(111, 19);
         this.checkBox4.TabIndex = 18;
         this.checkBox4.Text = "DebugTemplate";
         this.checkBox4.UseVisualStyleBackColor = true;
         // 
         // checkBox2
         // 
         this.checkBox2.AutoSize = true;
         this.checkBox2.Location = new System.Drawing.Point(17, 173);
         this.checkBox2.Name = "checkBox2";
         this.checkBox2.Size = new System.Drawing.Size(140, 19);
         this.checkBox2.TabIndex = 16;
         this.checkBox2.Text = "MaxAddsToMaxEmits";
         this.checkBox2.UseVisualStyleBackColor = true;
         // 
         // cbRetryErrors
         // 
         this.cbRetryErrors.AutoSize = true;
         this.cbRetryErrors.Location = new System.Drawing.Point(17, 147);
         this.cbRetryErrors.Name = "cbRetryErrors";
         this.cbRetryErrors.Size = new System.Drawing.Size(83, 19);
         this.cbRetryErrors.TabIndex = 15;
         this.cbRetryErrors.Text = "RetryErrors";
         this.cbRetryErrors.UseVisualStyleBackColor = true;
         // 
         // cbIgnoreErrors
         // 
         this.cbIgnoreErrors.AutoSize = true;
         this.cbIgnoreErrors.Location = new System.Drawing.Point(17, 122);
         this.cbIgnoreErrors.Name = "cbIgnoreErrors";
         this.cbIgnoreErrors.Size = new System.Drawing.Size(90, 19);
         this.cbIgnoreErrors.TabIndex = 14;
         this.cbIgnoreErrors.Text = "IgnoreErrors";
         this.cbIgnoreErrors.UseVisualStyleBackColor = true;
         // 
         // cbIgnoreLimited
         // 
         this.cbIgnoreLimited.AutoSize = true;
         this.cbIgnoreLimited.Location = new System.Drawing.Point(17, 97);
         this.cbIgnoreLimited.Name = "cbIgnoreLimited";
         this.cbIgnoreLimited.Size = new System.Drawing.Size(100, 19);
         this.cbIgnoreLimited.TabIndex = 13;
         this.cbIgnoreLimited.Text = "IgnoreLimited";
         this.cbIgnoreLimited.UseVisualStyleBackColor = true;
         // 
         // cbTraceValues
         // 
         this.cbTraceValues.AutoSize = true;
         this.cbTraceValues.Location = new System.Drawing.Point(17, 72);
         this.cbTraceValues.Name = "cbTraceValues";
         this.cbTraceValues.Size = new System.Drawing.Size(89, 19);
         this.cbTraceValues.TabIndex = 12;
         this.cbTraceValues.Text = "TraceValues";
         this.cbTraceValues.UseVisualStyleBackColor = true;
         // 
         // cbDoNotRename
         // 
         this.cbDoNotRename.AutoSize = true;
         this.cbDoNotRename.Location = new System.Drawing.Point(17, 47);
         this.cbDoNotRename.Name = "cbDoNotRename";
         this.cbDoNotRename.Size = new System.Drawing.Size(104, 19);
         this.cbDoNotRename.TabIndex = 11;
         this.cbDoNotRename.Text = "DoNotRename";
         this.cbDoNotRename.UseVisualStyleBackColor = true;
         // 
         // CbFullImport
         // 
         this.CbFullImport.AutoSize = true;
         this.CbFullImport.Location = new System.Drawing.Point(17, 22);
         this.CbFullImport.Name = "CbFullImport";
         this.CbFullImport.Size = new System.Drawing.Size(81, 19);
         this.CbFullImport.TabIndex = 10;
         this.CbFullImport.Text = "FullImport";
         this.CbFullImport.UseVisualStyleBackColor = true;
         // 
         // button3
         // 
         this.button3.Location = new System.Drawing.Point(602, 81);
         this.button3.Name = "button3";
         this.button3.Size = new System.Drawing.Size(50, 23);
         this.button3.TabIndex = 8;
         this.button3.Text = "redo";
         this.button3.UseVisualStyleBackColor = true;
         this.button3.Click += new System.EventHandler(this.button3_Click);
         // 
         // button4
         // 
         this.button4.Location = new System.Drawing.Point(577, 110);
         this.button4.Name = "button4";
         this.button4.Size = new System.Drawing.Size(75, 23);
         this.button4.TabIndex = 9;
         this.button4.Text = "Open dir";
         this.button4.UseVisualStyleBackColor = true;
         this.button4.Click += new System.EventHandler(this.button4_Click);
         // 
         // button5
         // 
         this.button5.Location = new System.Drawing.Point(577, 139);
         this.button5.Name = "button5";
         this.button5.Size = new System.Drawing.Size(75, 23);
         this.button5.TabIndex = 10;
         this.button5.Text = "Edit XML";
         this.button5.UseVisualStyleBackColor = true;
         this.button5.Click += new System.EventHandler(this.button5_Click);
         // 
         // timer1
         // 
         this.timer1.Interval = 500;
         this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
         // 
         // btnCancel
         // 
         this.btnCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
         this.btnCancel.Enabled = false;
         this.btnCancel.Location = new System.Drawing.Point(122, 82);
         this.btnCancel.Name = "btnCancel";
         this.btnCancel.Size = new System.Drawing.Size(75, 26);
         this.btnCancel.TabIndex = 12;
         this.btnCancel.Text = "Cancel";
         this.btnCancel.UseVisualStyleBackColor = true;
         this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
         // 
         // label3
         // 
         this.label3.AutoSize = true;
         this.label3.Location = new System.Drawing.Point(233, 325);
         this.label3.Name = "label3";
         this.label3.Size = new System.Drawing.Size(71, 15);
         this.label3.TabIndex = 13;
         this.label3.Text = "MaxRecords";
         // 
         // txtMaxRecords
         // 
         this.txtMaxRecords.Location = new System.Drawing.Point(310, 322);
         this.txtMaxRecords.Name = "txtMaxRecords";
         this.txtMaxRecords.Size = new System.Drawing.Size(71, 23);
         this.txtMaxRecords.TabIndex = 14;
         this.txtMaxRecords.Text = "-1";
         // 
         // button1
         // 
         this.button1.Location = new System.Drawing.Point(277, 227);
         this.button1.Name = "button1";
         this.button1.Size = new System.Drawing.Size(79, 33);
         this.button1.TabIndex = 15;
         this.button1.Text = "Date";
         this.button1.UseVisualStyleBackColor = true;
         this.button1.Visible = false;
         this.button1.Click += new System.EventHandler(this.button1_Click_1);
         // 
         // textBox1
         // 
         this.textBox1.Location = new System.Drawing.Point(277, 168);
         this.textBox1.Name = "textBox1";
         this.textBox1.Size = new System.Drawing.Size(375, 23);
         this.textBox1.TabIndex = 16;
         this.textBox1.Visible = false;
         // 
         // textBox2
         // 
         this.textBox2.Location = new System.Drawing.Point(277, 198);
         this.textBox2.Name = "textBox2";
         this.textBox2.Size = new System.Drawing.Size(375, 23);
         this.textBox2.TabIndex = 17;
         this.textBox2.Visible = false;
         // 
         // checkBox1
         // 
         this.checkBox1.AutoSize = true;
         this.checkBox1.Location = new System.Drawing.Point(1109, 375);
         this.checkBox1.Name = "checkBox1";
         this.checkBox1.Size = new System.Drawing.Size(15, 14);
         this.checkBox1.TabIndex = 18;
         this.checkBox1.UseVisualStyleBackColor = true;
         this.checkBox1.CheckedChanged += new System.EventHandler(this.checkBox1_CheckedChanged);
         // 
         // label4
         // 
         this.label4.AutoSize = true;
         this.label4.Location = new System.Drawing.Point(387, 325);
         this.label4.Name = "label4";
         this.label4.Size = new System.Drawing.Size(58, 15);
         this.label4.TabIndex = 20;
         this.label4.Text = "MaxEmits";
         // 
         // txtMaxEmits
         // 
         this.txtMaxEmits.Location = new System.Drawing.Point(451, 322);
         this.txtMaxEmits.Name = "txtMaxEmits";
         this.txtMaxEmits.Size = new System.Drawing.Size(70, 23);
         this.txtMaxEmits.TabIndex = 21;
         this.txtMaxEmits.Text = "-1";
         // 
         // label6
         // 
         this.label6.AutoSize = true;
         this.label6.Location = new System.Drawing.Point(233, 354);
         this.label6.Name = "label6";
         this.label6.Size = new System.Drawing.Size(55, 15);
         this.label6.TabIndex = 24;
         this.label6.Text = "Endpoint";
         // 
         // label7
         // 
         this.label7.AutoSize = true;
         this.label7.Location = new System.Drawing.Point(233, 383);
         this.label7.Name = "label7";
         this.label7.Size = new System.Drawing.Size(49, 15);
         this.label7.TabIndex = 25;
         this.label7.Text = "Pipeline";
         // 
         // cbEndpoints
         // 
         this.cbEndpoints.FormattingEnabled = true;
         this.cbEndpoints.Location = new System.Drawing.Point(309, 351);
         this.cbEndpoints.Name = "cbEndpoints";
         this.cbEndpoints.Size = new System.Drawing.Size(167, 23);
         this.cbEndpoints.TabIndex = 26;
         // 
         // cbPipeLines
         // 
         this.cbPipeLines.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
         this.cbPipeLines.FormattingEnabled = true;
         this.cbPipeLines.Location = new System.Drawing.Point(309, 380);
         this.cbPipeLines.Name = "cbPipeLines";
         this.cbPipeLines.Size = new System.Drawing.Size(169, 23);
         this.cbPipeLines.TabIndex = 27;
         // 
         // gridStatus
         // 
         this.gridStatus.AllowUserToAddRows = false;
         this.gridStatus.AllowUserToDeleteRows = false;
         this.gridStatus.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.DisplayedCells;
         this.gridStatus.AutoSizeRowsMode = System.Windows.Forms.DataGridViewAutoSizeRowsMode.DisplayedCells;
         this.gridStatus.BackgroundColor = System.Drawing.Color.White;
         this.gridStatus.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
         this.gridStatus.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
         this.gridStatus.ColumnHeadersVisible = false;
         this.gridStatus.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.Datasource,
            this.Message});
         this.gridStatus.EditMode = System.Windows.Forms.DataGridViewEditMode.EditProgrammatically;
         this.gridStatus.GridColor = System.Drawing.SystemColors.Control;
         this.gridStatus.Location = new System.Drawing.Point(30, 453);
         this.gridStatus.MultiSelect = false;
         this.gridStatus.Name = "gridStatus";
         this.gridStatus.ReadOnly = true;
         this.gridStatus.RowHeadersVisible = false;
         this.gridStatus.Size = new System.Drawing.Size(1095, 139);
         this.gridStatus.TabIndex = 28;
         // 
         // Datasource
         // 
         this.Datasource.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.AllCells;
         this.Datasource.HeaderText = "Datasource";
         this.Datasource.Name = "Datasource";
         this.Datasource.ReadOnly = true;
         this.Datasource.Width = 5;
         // 
         // Message
         // 
         this.Message.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.AllCells;
         this.Message.HeaderText = "Message";
         this.Message.Name = "Message";
         this.Message.ReadOnly = true;
         this.Message.Width = 5;
         // 
         // txtSwitches
         // 
         this.txtSwitches.Location = new System.Drawing.Point(310, 409);
         this.txtSwitches.Name = "txtSwitches";
         this.txtSwitches.Size = new System.Drawing.Size(370, 23);
         this.txtSwitches.TabIndex = 29;
         // 
         // label5
         // 
         this.label5.AutoSize = true;
         this.label5.Location = new System.Drawing.Point(233, 412);
         this.label5.Name = "label5";
         this.label5.Size = new System.Drawing.Size(49, 15);
         this.label5.TabIndex = 30;
         this.label5.Text = "Swiches";
         // 
         // btnOpen
         // 
         this.btnOpen.Location = new System.Drawing.Point(621, 50);
         this.btnOpen.Name = "btnOpen";
         this.btnOpen.Size = new System.Drawing.Size(31, 23);
         this.btnOpen.TabIndex = 31;
         this.btnOpen.Text = "...";
         this.btnOpen.UseVisualStyleBackColor = true;
         this.btnOpen.Click += new System.EventHandler(this.btnOpen_Click);
         // 
         // Form1
         // 
         this.AcceptButton = this.btnImport;
         this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
         this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
         this.CancelButton = this.btnCancel;
         this.ClientSize = new System.Drawing.Size(1154, 604);
         this.Controls.Add(this.btnOpen);
         this.Controls.Add(this.label5);
         this.Controls.Add(this.txtSwitches);
         this.Controls.Add(this.gridStatus);
         this.Controls.Add(this.cbPipeLines);
         this.Controls.Add(this.cbEndpoints);
         this.Controls.Add(this.label7);
         this.Controls.Add(this.label6);
         this.Controls.Add(this.txtMaxEmits);
         this.Controls.Add(this.label4);
         this.Controls.Add(this.checkBox1);
         this.Controls.Add(this.textBox2);
         this.Controls.Add(this.textBox1);
         this.Controls.Add(this.button1);
         this.Controls.Add(this.txtMaxRecords);
         this.Controls.Add(this.label3);
         this.Controls.Add(this.btnCancel);
         this.Controls.Add(this.button5);
         this.Controls.Add(this.button4);
         this.Controls.Add(this.button3);
         this.Controls.Add(this.grpFlags);
         this.Controls.Add(this.label2);
         this.Controls.Add(this.label1);
         this.Controls.Add(this.dsList);
         this.Controls.Add(this.btnImport);
         this.Controls.Add(this.comboBox1);
         this.Font = new System.Drawing.Font("Segoe UI", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
         this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
         this.Name = "Form1";
         this.Text = "Datasource importer";
         this.Load += new System.EventHandler(this.Form1_Load);
         this.grpFlags.ResumeLayout(false);
         this.grpFlags.PerformLayout();
         ((System.ComponentModel.ISupportInitialize)(this.gridStatus)).EndInit();
         this.ResumeLayout(false);
         this.PerformLayout();

      }

      #endregion

      private System.Windows.Forms.ComboBox comboBox1;
      private System.Windows.Forms.Button btnImport;
      private System.Windows.Forms.OpenFileDialog openFileDialog1;
      private System.Windows.Forms.CheckedListBox dsList;
      private System.Windows.Forms.Label label1;
      private System.Windows.Forms.Label label2;
      private System.Windows.Forms.GroupBox grpFlags;
      private System.Windows.Forms.Button button3;
      private System.Windows.Forms.CheckBox cbTraceValues;
      private System.Windows.Forms.CheckBox cbDoNotRename;
      private System.Windows.Forms.CheckBox CbFullImport;
      private System.Windows.Forms.Button button4;
      private System.Windows.Forms.Button button5;
      private System.Windows.Forms.Timer timer1;
      private System.Windows.Forms.Button btnCancel;
      private System.Windows.Forms.CheckBox cbIgnoreErrors;
      private System.Windows.Forms.CheckBox cbIgnoreLimited;
      private System.Windows.Forms.Label label3;
      private System.Windows.Forms.TextBox txtMaxRecords;
      private System.Windows.Forms.Button button1;
      private System.Windows.Forms.TextBox textBox1;
      private System.Windows.Forms.TextBox textBox2;
      private System.Windows.Forms.CheckBox checkBox1;
      private System.Windows.Forms.CheckBox cbRetryErrors;
      private System.Windows.Forms.CheckBox checkBox2;
      private System.Windows.Forms.Label label4;
      private System.Windows.Forms.TextBox txtMaxEmits;
      private System.Windows.Forms.Label label6;
      private System.Windows.Forms.Label label7;
      private System.Windows.Forms.ComboBox cbEndpoints;
      private System.Windows.Forms.ComboBox cbPipeLines;
      private System.Windows.Forms.CheckBox checkBox4;
      private System.Windows.Forms.CheckBox checkBox5;
      private System.Windows.Forms.DataGridView gridStatus;
      private System.Windows.Forms.DataGridViewTextBoxColumn Datasource;
      private System.Windows.Forms.DataGridViewTextBoxColumn Message;
      private System.Windows.Forms.CheckBox checkBox3;
      private System.Windows.Forms.TextBox txtSwitches;
      private System.Windows.Forms.Label label5;
      private System.Windows.Forms.Button btnOpen;
   }
}

