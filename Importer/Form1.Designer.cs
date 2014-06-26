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
         this.cbIgnoreErrors = new System.Windows.Forms.CheckBox();
         this.cbIgnoreLimited = new System.Windows.Forms.CheckBox();
         this.cbTraceValues = new System.Windows.Forms.CheckBox();
         this.cbDoNotRename = new System.Windows.Forms.CheckBox();
         this.CbFullImport = new System.Windows.Forms.CheckBox();
         this.button3 = new System.Windows.Forms.Button();
         this.button4 = new System.Windows.Forms.Button();
         this.button5 = new System.Windows.Forms.Button();
         this.button6 = new System.Windows.Forms.Button();
         this.timer1 = new System.Windows.Forms.Timer(this.components);
         this.btnCancel = new System.Windows.Forms.Button();
         this.label3 = new System.Windows.Forms.Label();
         this.txtMaxRecords = new System.Windows.Forms.TextBox();
         this.button1 = new System.Windows.Forms.Button();
         this.grpFlags.SuspendLayout();
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
         this.btnImport.Location = new System.Drawing.Point(30, 99);
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
         this.grpFlags.Controls.Add(this.cbIgnoreErrors);
         this.grpFlags.Controls.Add(this.cbIgnoreLimited);
         this.grpFlags.Controls.Add(this.cbTraceValues);
         this.grpFlags.Controls.Add(this.cbDoNotRename);
         this.grpFlags.Controls.Add(this.CbFullImport);
         this.grpFlags.Location = new System.Drawing.Point(30, 155);
         this.grpFlags.Name = "grpFlags";
         this.grpFlags.Size = new System.Drawing.Size(165, 160);
         this.grpFlags.TabIndex = 7;
         this.grpFlags.TabStop = false;
         this.grpFlags.Text = "Flags";
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
         // button6
         // 
         this.button6.Location = new System.Drawing.Point(623, 52);
         this.button6.Name = "button6";
         this.button6.Size = new System.Drawing.Size(29, 23);
         this.button6.TabIndex = 11;
         this.button6.Text = "...";
         this.button6.UseVisualStyleBackColor = true;
         this.button6.Click += new System.EventHandler(this.button6_Click);
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
         this.btnCancel.Location = new System.Drawing.Point(123, 99);
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
         this.label3.Location = new System.Drawing.Point(27, 328);
         this.label3.Name = "label3";
         this.label3.Size = new System.Drawing.Size(71, 15);
         this.label3.TabIndex = 13;
         this.label3.Text = "MaxRecords";
         // 
         // txtMaxRecords
         // 
         this.txtMaxRecords.Location = new System.Drawing.Point(104, 325);
         this.txtMaxRecords.Name = "txtMaxRecords";
         this.txtMaxRecords.Size = new System.Drawing.Size(94, 23);
         this.txtMaxRecords.TabIndex = 14;
         this.txtMaxRecords.Text = "-1";
         // 
         // button1
         // 
         this.button1.Location = new System.Drawing.Point(248, 92);
         this.button1.Name = "button1";
         this.button1.Size = new System.Drawing.Size(79, 33);
         this.button1.TabIndex = 15;
         this.button1.Text = "button1";
         this.button1.UseVisualStyleBackColor = true;
         this.button1.Visible = false;
         this.button1.Click += new System.EventHandler(this.button1_Click_1);
         // 
         // Form1
         // 
         this.AcceptButton = this.btnImport;
         this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
         this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
         this.CancelButton = this.btnCancel;
         this.ClientSize = new System.Drawing.Size(1143, 401);
         this.Controls.Add(this.button1);
         this.Controls.Add(this.txtMaxRecords);
         this.Controls.Add(this.label3);
         this.Controls.Add(this.btnCancel);
         this.Controls.Add(this.button6);
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
      private System.Windows.Forms.Button button6;
      private System.Windows.Forms.Timer timer1;
      private System.Windows.Forms.Button btnCancel;
      private System.Windows.Forms.CheckBox cbIgnoreErrors;
      private System.Windows.Forms.CheckBox cbIgnoreLimited;
      private System.Windows.Forms.Label label3;
      private System.Windows.Forms.TextBox txtMaxRecords;
      private System.Windows.Forms.Button button1;
   }
}

