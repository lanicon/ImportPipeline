using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.Core
{
   public class ReflectedMember<T> : ReflectedMember where T : MemberInfo
   {
      public new readonly T Member;
      public ReflectedMember (Type t, MemberInfo m): base (t, m)
      {
         Member = (T)m;
      }
   }
   public class ReflectedMember
   {
      public readonly Type Type;
      public readonly MemberInfo Member;

      public ReflectedMember(Type t, MemberInfo m)
      {
         Type = t;
         Member = m;
      }

      public override int GetHashCode()
      {
         return Type.GetHashCode() ^ Member.GetHashCode();
      }
      public override bool Equals(object obj)
      {
         if (obj == null) return false;
         Type t = obj as Type;
         if (t!=null) return Type.Equals (t);

         ReflectedMember rc = obj as ReflectedMember;
         if (rc != null) return Type.Equals(rc.Type) && Member.Equals(rc.Member);
         return false;
      }

      //public static bool operator !=(ReflectedMember a, ReflectedMember b)
      //{
      //   return ! operator == (a, b);
      //}
      //public static bool operator ==(ReflectedMember a, ReflectedMember b);

      



      class _Creater
      {
         String expr;
         public MemberInfo member;
         //MemberTypes filter;

         public _Creater(Type t, String name, MemberTypes filter, BindingFlags flags = BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance)
         {
            expr = name;
            t.FindMembers(filter, BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance, doFilter, null);

         }

         private bool doFilter(MemberInfo m, object filterCriteria)
         {
            if (!expr.Equals(m.Name, StringComparison.OrdinalIgnoreCase)) return false;
            switch (m.MemberType)
            {
               case MemberTypes.Field:
                  if (member == null || m.Name == expr)
                     member = (FieldInfo)m;
                  break;
               case MemberTypes.Property:
                  PropertyInfo pi = (PropertyInfo)m;
                  if (pi.GetIndexParameters().Length != 0)break;
                  if (member == null || m.Name == expr)
                     member = pi;
                  break;
               case MemberTypes.Method:
                  MethodInfo mi = (MethodInfo)m;
                  if (mi.GetParameters().Length != 0) break;
                  if (member == null || m.Name == expr)
                     member = mi;
                 break;
            }
            return false;
         }
      }
      public static ReflectedMember Create(Type t, String name, MemberTypes filter, BindingFlags flags = BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance)
      {
         var c = new _Creater (t, name, filter, flags);
         if (c.member == null) return null;
         return new ReflectedMember(t, c.member);
      }

   }
}
