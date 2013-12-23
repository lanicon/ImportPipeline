using Bitmanager.Core;
using Bitmanager.Xml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;

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



      private static Type stringToType(String typeName)
      {
         if (String.IsNullOrEmpty(typeName)) throw new BMException("CreateObject() failed: typeName cannot be empty.");
         Type t = FindType(typeName);
         if (t == null) throw new BMException("CreateObject({0}) failed: type not found.", typeName);
         return t;
      }

      public static Object CreateObject(String typeName)
      {
         Type t = stringToType (typeName);
         try
         {
            return Activator.CreateInstance(t);
         }
         catch (Exception e)
         {
            throw new BMException("CreateObject({0}) failed: {1}", typeName, e.Message);
         }
      }

      public static Object CreateObject(String typeName, params Object[] parms)
      {
         Type t = stringToType(typeName);

         int paramCnt = parms==null ? 0 : parms.Length;
         Object[] arr = parms;
         while (paramCnt >= 0)
         {
            try
            {
               return Activator.CreateInstance(t, arr);
            }
            catch (Exception e)
            {
               paramCnt--;
               if (e is MissingMethodException && paramCnt >= 0)
               {
                  if (paramCnt == 0)
                     arr = null;
                  else
                  {
                     arr = new Object[paramCnt];
                     Array.Copy(parms, arr, paramCnt);
                  }
               }
               throw new BMException("CreateObject({0}) failed: {1}", typeName, e.Message);
            }
         }
         return null; //to keep compiler happy... Cannot happen
      }

      public static Object CreateObject(XmlNode node)
      {
         return CreateObject(readType(node));
      }

      private static String readType(XmlNode node)
      {
         return node.ReadStr("@type");
      }
      public static Object CreateObject(XmlNode node, params Object[] parms)
      {
         return CreateObject(readType(node), parms);
      }


      public static T CreateObject<T>(String typeName) where T : class
      {
         return Cast<T>(typeName, CreateObject(typeName));
      }

      public static T CreateObject<T>(String typeName, params Object[] parms) where T : class
      {
         return Cast<T>(typeName, CreateObject(typeName, parms));
      }

      public static T CreateObject<T>(XmlNode node) where T : class
      {
         String typeName = readType(node);
         return Cast<T>(typeName, CreateObject(typeName));
      }

      public static T CreateObject<T>(XmlNode node, params Object[] parms) where T: class
      {
         String typeName = readType(node);
         return Cast<T> (typeName, CreateObject(typeName, parms));
      }

      public static T Cast<T>(String typeName, Object obj) where T: class
      {
         T ret = obj as T;
         if (ret != null) return ret;
         throw new BMException("Object '{0}' (type={1}) cannot be casted to '{2}'.", typeName, obj.GetType().FullName, typeof(T).FullName);
      }

      private static Object checkType(String typeName, Object obj, Type t)
      {
         Type objType = obj.GetType();
         if (t.IsAssignableFrom(objType)) return obj;
         throw new BMException("Object '{0}' (type={1}) cannot be casted to '{2}'.", typeName, objType.FullName, t.FullName);
      }

   }


}
