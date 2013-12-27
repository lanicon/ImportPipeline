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

      private static Type findExactType(Assembly a, String typeName)
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
      private static Type findNonExactType(Assembly a, String typeName)
      {
         String lcType = typeName.ToLowerInvariant();
         Logs.DebugLog.Log("Searching for '{0}'", lcType);
         try
         {
            Type[] arr = a.GetTypes();
            for (int i = 0; i < arr.Length; i++)
            {
               Type t = arr[i];
               if (!t.IsClass) continue;
               String fullName = t.FullName.ToLowerInvariant();
               Logs.DebugLog.Log("-- '{0}'", fullName);
               if (fullName.Length < lcType.Length) continue;
               if (!fullName.EndsWith(lcType)) continue;

               if (fullName.Length == lcType.Length) return t;
               if (fullName[fullName.Length - lcType.Length - 1]=='.') return t;
               //Logs.DebugLog.Log("-- -- [{0}]: {1}", fullName.Length - lcType.Length - 1, fullName[fullName.Length - lcType.Length - 1]);
            }
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
         String typeName;
         Type t;
         if (arr.Length == 1)
         {
            typeName = (objId[0] == '@') ? objId.Substring(1) : objId;
            var asms = AppDomain.CurrentDomain.GetAssemblies();
            foreach (var asm in asms)
            {
               t = findExactType(asm, typeName);
               if (t != null) return t;
            }
            if (objId[0] == '@') return null;
            foreach (var asm in asms)
            {
               t = findNonExactType(asm, typeName);
               if (t != null) return t;
            }
            return null;
         }

         //Need to load the assembly
         Assembly asm2 = Assembly.Load(arr[0]);
         typeName = (arr[1][0] == '@') ? arr[1].Substring(1) : arr[1];
         t = findExactType(asm2, typeName);
         if (t != null || arr[1][0] == '@') return t;
         return findNonExactType(asm2, typeName);
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
               throw new BMException(e, "CreateObject({0}) failed: {1}", typeName, e.GetBestMessage());
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


      //public MethodInfo FindMethod(Object obj, String name)
      //{
      //   Type t = obj.GetType();
      //   return t.GetMethod(name, BindingFlags.IgnoreCase | BindingFlags.Public);
      //}
      //public MethodInfo FindMethod(Object obj, String name, Type[] parms)
      //{
      //   Type t = obj.GetType();
      //   MethodInfo mi = null;
      //   mi.Invoke(.GetParameters()[0].}..
      //   t.GetMethods();
      //   return t.GetMethod(name, BindingFlags.IgnoreCase | BindingFlags.Public);
      //}

   }


}
