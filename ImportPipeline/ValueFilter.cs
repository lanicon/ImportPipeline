using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   interface IValueFilter
   {
      bool IsFiltered(Object value);
   }

   public class ValueFilterStringElt: IValueFilter
   {
      //private String incl, excl;

      public bool IsFiltered(object value)
      {
         return false;
         //if 
         //JArray arr = r.
         //throw new NotImplementedException();
      }
   }
}
