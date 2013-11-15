using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class Objects
   {
      private static readonly char[] SPLIT_CHAR = { '#' };

      private static Type findType(Assembly a, String typeName)
      {
         try
         {
            return a.GetType(typeName, false, true);
         }
         catch
         {
         }
         return null;
      }

      public static Type FindType(String objId)
      {
         if (String.IsNullOrEmpty(objId)) throw new BMException("GetType() failed: progid cannot be empty.");
         String[] arr = objId.Split(SPLIT_CHAR);
         if (arr.Length == 1)
         {
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
               Type t = findType(asm, arr[0]);
               if (t != null) return t;
            }
            return null;
         }

         //Need to load the assembly
         Assembly asm2 = Assembly.Load(arr[0]);
         return findType(asm2, arr[1]);
      }
      public static Object CreateObject(String objId)
      {
         if (String.IsNullOrEmpty(objId)) throw new BMException("CreateObject() failed: progid cannot be empty.");
         Type t = FindType(objId);
         if (t == null) throw new BMException("CreateObject({0}) failed: type not found.", objId);

         try
         {
            return Activator.CreateInstance(t);
         }
         catch (Exception e)
         {
            throw new BMException("CreateObject({0}) failed: {1}", objId, e.Message);
         }
      }
   }
}
