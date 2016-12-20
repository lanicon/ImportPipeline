using Bitmanager.Core;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Bitmanager.Importer
{
   public class Autocompleter_Elt
   {
      public readonly String MainName;
      public readonly String AltName;
      public readonly String DisplayName;

      public Autocompleter_Elt(String full, String main)
      {
         DisplayName = full;
         AltName = full.ToLowerInvariant();
         MainName = main == null ? AltName : main.ToLowerInvariant();
      }

      public override string ToString()
      {
         return DisplayName;
      }
      public virtual float ComputeScore(String arg)
      {
         return Math.Max(ComputeScore(arg, this.MainName), 0.5f * ComputeScore(arg, this.AltName));
      }
      public virtual float ComputeScore(String arg, String toCompare)
      {
         int idx = toCompare.IndexOf(arg);
         if (idx==0)
            return 2 + (float)arg.Length / toCompare.Length;
         if (idx>0)
            return 1 + (float)arg.Length / toCompare.Length;

         float tot = arg.Length + toCompare.Length;
         tot = (tot - EditDistance(arg, toCompare)) / tot;
         //Logs.DebugLog.Log ("-- {0}<->{1}: {2}", arg, toCompare, tot);
         return tot;
      }
      public float score;

      public static int EditDistance(string arg, string toCompare)
      {
         int len_orig = arg.Length;
         int len_diff = toCompare.Length;

         var matrix = new int[len_orig + 1, len_diff + 1];
         for (int i = 0; i <= len_orig; i++)
            matrix[i, 0] = i;
         for (int j = 0; j <= len_diff; j++)
            matrix[0, j] = j;

         for (int i = 1; i <= len_orig; i++)
         {
            for (int j = 1; j <= len_diff; j++)
            {
               int cost = toCompare[j - 1] == arg[i - 1] ? 0 : 1;
               var vals = new int[] {
				matrix[i - 1, j] + 1,
				matrix[i, j - 1] + 1,
				matrix[i - 1, j - 1] + cost
			};
               matrix[i, j] = vals.Min();
               if (i > 1 && j > 1 && arg[i - 1] == toCompare[j - 2] && arg[i - 2] == toCompare[j - 1])
                  matrix[i, j] = Math.Min(matrix[i, j], matrix[i - 2, j - 2] + cost);
            }
         }
         return matrix[len_orig, len_diff];
      }
   }




   public class AutoCompleter
   {
      private Logger logger;
      public delegate Autocompleter_Elt FN_TO_AUTOCOMPLETER_ELT (String s);
      protected readonly List<Autocompleter_Elt> list;
      private readonly ComboBox cb;
      private FN_TO_AUTOCOMPLETER_ELT fnConvert;
      private int lastSend; //Used to rollup posted msgs

      private readonly KeyPressEventHandler keyPressHandler;
      private readonly EventHandler textHandler;
      private readonly EventHandler selIndexHandler;
      private readonly SubclassHook hook;
      public AutoCompleter(ComboBox cb, List<String> list) : this(cb, list, DefaultConverter) { }
      public AutoCompleter(ComboBox cb, List<String> list, FN_TO_AUTOCOMPLETER_ELT converter)
      {
         this.cb = cb;
         this.list = toEltList(list, converter);
         this.fnConvert = converter;

         cb.KeyPress += keyPressHandler = handleKeyPress;
         cb.TextChanged += textHandler = handleTextUpdate;
         cb.SelectedIndexChanged += selIndexHandler = handleSelIndexChanged;
         populateItems(this.list);
         logger = Logs.CreateLogger("ac", "ac");
         hook = new SubclassHook(this);
      }
      public void Dispose()
      {
         cb.KeyPress -= keyPressHandler;
         cb.TextChanged -= textHandler;
         cb.SelectedIndexChanged -= selIndexHandler;
         hook.Dispose();
      }

      private static List<Autocompleter_Elt> toEltList (ComboBox cb, FN_TO_AUTOCOMPLETER_ELT cnv)
      {
         var items = cb.Items;
         var ret = new List<Autocompleter_Elt>(items.Count);
         foreach (var x in items) ret.Add(cnv(x.ToString()));
         return ret;
      }
      private static List<Autocompleter_Elt> toEltList(List<String> list, FN_TO_AUTOCOMPLETER_ELT cnv)
      {
         var ret = new List<Autocompleter_Elt>(list.Count);
         foreach (var x in list) ret.Add(cnv(x));
         return ret;
      }
      public static Autocompleter_Elt DefaultConverter(String s)
      {
         return new Autocompleter_Elt(s, null);
      }
      public static Autocompleter_Elt FileNameConverter(String s)
      {
         return new Autocompleter_Elt(s, Path.GetFileName(s));
      }
      public static Autocompleter_Elt DirNameConverter(String s)
      {
         return new Autocompleter_Elt(s, Path.GetFileName(Path.GetDirectoryName(s)));
      }

      public bool PushSelectedItem()
      {
         dumpItems(cb, "push");
         int idx = cb.SelectedIndex;
         if (idx >= 0)
         {
            int list_ix;
            Object item = cb.Items[idx];
            var elt = item as Autocompleter_Elt;
            if (elt != null)
               list_ix = list.IndexOf(elt); 
            else
            {
               String arg = item.ToString();
               for (list_ix = list.Count - 1; list_ix >= 0; list_ix--)
               {
                  if (arg == list[list_ix].ToString()) break;
               }
            }
            if (list_ix >= 0)
            {
               Autocompleter_Elt x = list[list_ix];
               list.RemoveAt(list_ix);
               list.Insert(0, x);
               return true;
            }
         }
         return false;
      }

      protected virtual void Push (List<Autocompleter_Elt> list, int ix)
      {
         if (ix > 0)
         {
            Autocompleter_Elt x = list[ix];
            list.RemoveAt(ix);
            list.Insert(0, x);
         }
      }


      public bool PushSelectedItem(String regkey, RegistryKey root=null )
      {
         if (!PushSelectedItem()) return false;
         var list = this.list.Select(x=>x.DisplayName).ToList();
         History.SaveHistory(list, regkey, null, root);
         return true;
      }

      public int AddItems(List<String> list)
      {
         int ret = list.Count;
         for (int i = 0; i < list.Count; i++)
         {
            this.list.Add(this.fnConvert(list[i]));
         }
         populateItems(this.list);
         return ret;
      }

      public int AddItem(String item)
      {
         int ret = list.Count;
         list.Add(this.fnConvert(item));
         populateItems(list);
         return ret;
      }



      private void handleSelIndexChanged(object sender, EventArgs e)
      {
      }

      protected virtual void handleKeyPress(object sender, KeyPressEventArgs e)
      {
         cb.DroppedDown = true;
      }

      [return: MarshalAs(UnmanagedType.Bool)]
      [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
      static extern bool PostMessage(IntPtr hWnd, uint Msg, IntPtr wParam, IntPtr lParam);

      private void handleTextUpdate(object sender, EventArgs e)
      {
         if (cb.DroppedDown)
         {
            ++lastSend;
            PostMessage(cb.Handle, 0x8001, IntPtr.Zero, new IntPtr(lastSend));
         }
      }

      internal void handleMessage(Message m)
      {
         if (m.Msg == 0x8001)
            if (m.LParam.ToInt32() >= lastSend)
               computeScoreAndPopulate(cb.Text);
      }


      private void computeScoreAndPopulate (String arg) 
      {
         if (String.IsNullOrEmpty(arg)) 
         {
            populateItems(list);
            return;
         }

         arg = arg.ToLowerInvariant();
         var scoredList = new List<Autocompleter_Elt>(list.Count);
         float recencyScore = 1.0f;
         for (int i=0; i<list.Count; i++)
         {
            var elt = list[i];
            scoredList.Add(elt);
            elt.score = elt.ComputeScore(arg) +0.1f * recencyScore;
            recencyScore *= 0.9f;
            //Logs.DebugLog.Log("{0}<->{1}: {2}", arg, elt.DisplayName, elt.score);
         }
         scoredList.Sort (sortScoreCb);
         populateItems(scoredList);
      }

      private static int sortScoreCb (Autocompleter_Elt a, Autocompleter_Elt b)
      {
         int ret = Comparer<float>.Default.Compare(b.score, a.score);
         if (ret != 0) return ret;
         return 0;
      }

      private void dumpItems(ComboBox cb, String reason)
      {
         Logger logger = Logs.CreateLogger("ac");
         var items = cb.Items;
         logger.Log ("Dumping {0} items: {1}", items.Count, reason);
         foreach (var x in items)
         {
            logger.Log("-- type={0}, txt={1}", x.GetType().Name, x);
         }
      }
      private void populateItems(List<Autocompleter_Elt> list)
      {
         cb.BeginUpdate();
         var items = cb.Items;
         if (items.Count==list.Count)
         {
            for (int i = 0; i < list.Count; i++)
               items[i] = list[i];
         } 
         else 
         {
            items.Clear();
            foreach (var x in list) items.Add(x);
         }
         cb.EndUpdate();
      }

      const uint OUR_MSG = 0x8001;


      class SubclassHook: NativeWindow
      {
         private readonly AutoCompleter parent;
         private bool disposed;
         public SubclassHook (AutoCompleter parent)
         {
            this.parent = parent;
            this.AssignHandle(parent.cb.Handle);
         }

         public void Dispose()
         {
            if (!disposed)
            {
               disposed = true;
               ReleaseHandle();
            }
         }
         protected override void WndProc(ref Message m)
         {
            if (m.Msg == OUR_MSG)
            {
               parent.handleMessage (m);
               return;
            }
            base.WndProc(ref m);
         }
      }
   }
}
