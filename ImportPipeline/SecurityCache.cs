using Bitmanager.Core;
using System;
using System.Collections.Generic;
using System.DirectoryServices.AccountManagement;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text;

namespace Bitmanager.ImportPipeline
{
   public class SecurityAccount: IDisposable
   {
      public readonly NTAccount NTAccount;
      public readonly SecurityIdentifier Sid;
      public readonly WellKnownSidType? WellKnownSid;
      public readonly bool IsAccoundSid;
      public readonly bool IsGroup;
      public readonly bool IsMapped;

      static WellKnownSidType[] wellKnownTypes;

      static SecurityAccount()
      {
         Array vals = Enum.GetValues(typeof(WellKnownSidType));
         WellKnownSidType[] types = new WellKnownSidType[vals.Length];
         for (int i = 0; i < vals.Length; i++)
            types[i] = (WellKnownSidType)vals.GetValue(i);
         wellKnownTypes = types;
      }

      internal protected SecurityAccount(SecurityCache parent, IdentityReference ident)
      {
         NTAccount = ident as NTAccount;
         if (NTAccount != null)
         {
            Sid = (SecurityIdentifier)NTAccount.Translate(typeof(SecurityIdentifier));
            IsMapped = true;
         }
         else
         {
            Sid = (SecurityIdentifier)ident;
            try
            {
               NTAccount = (NTAccount)Sid.Translate(typeof(NTAccount));
               IsMapped = true;
            }
            catch (Exception e)
            {
               NTAccount = new NTAccount("unknown", Sid.Value);
            }
         }
         IsAccoundSid = Sid.IsAccountSid();

         for (int i=0; i<wellKnownTypes.Length; i++)
         {
            if (!Sid.IsWellKnown(wellKnownTypes[i])) continue;
            WellKnownSid = wellKnownTypes[i];
            break;
         }

         IsGroup = IsMapped && isGroup(parent);

      }

      private char[] accountSplitChar = { '\\' };
      private bool isGroup(SecurityCache parent)
      {
         PrincipalContext ctx;
         GroupPrincipal grp = null;
         String[] parts = NTAccount.Value.Split(accountSplitChar);
         String domain, local;
         if (parts.Length < 2) {
            domain = null;
            local = parts[0];
         }
         else
         {
            domain = parts[0];
            local = parts[1];
         }

         try
         {
            if (!IsAccoundSid) return false;
            if (domain == null)
            {
               ctx = parent.GetPricipalContext("", ContextType.Machine);
               //Logs.ErrorLog.Log("ctxl=" + SecurityCache.CtxToString(ctx));
               grp = getGroup(ctx, local);
               return grp != null;
            }
            ctx = parent.GetPricipalContext(domain, ContextType.Machine, ContextType.Domain, ContextType.ApplicationDirectory);
            //Logs.ErrorLog.Log("ctxd=" + SecurityCache.CtxToString(ctx));
            grp = getGroup(ctx, local);
            return grp != null;
         }
         finally
         {
            Utils.FreeAndNil(ref grp);
         }
      }

      GroupPrincipal getGroup(PrincipalContext ctx, String name)
      {
         GroupPrincipal arg = null;
         PrincipalSearcher searcher = null;
         try
         {
            arg = new GroupPrincipal(ctx, name);
            searcher = new PrincipalSearcher(arg);
            return searcher.FindOne() as GroupPrincipal;
            //return GroupPrincipal.FindByIdentity(ctx, IdentityType.Name, name);
         }
         catch (PrincipalOperationException x1)
         {
            Logs.ErrorLog.Log("getGroup({2}, {3}): errord={0}, hr={1}", x1.ErrorCode, x1.HResult, ctx.Name, name);
            return null;
         }
         catch (COMException err)
         {
            if (err.HResult == -2147024843) //network path not found
               return null;
            throw;
         }
         finally
         {
            Utils.FreeAndNil(ref arg);
            Utils.FreeAndNil(ref searcher);
         }
      }


      public override string ToString()
      {
         return String.Format("SecurityAccount[{0}: {1}, WellKnown={2}, isAcc={3}, grp={4}]", NTAccount, Sid, WellKnownSid, IsAccoundSid, IsGroup);
      }
      public void Dispose()
      {
         Exception e=null;
         Utils.Free(NTAccount, ref e);
         Utils.Free(Sid, ref e);
      }

      public static SecurityAccount FactoryImpl(SecurityCache parent, IdentityReference ident)
      {
         return new TikaSecurityAccount(parent, ident);
      }

   }

   public class SecurityCache: IDisposable
   {
      private readonly Func<SecurityCache, IdentityReference, SecurityAccount> factory;
      private List<SecurityAccount> accountList;
      private StringDict<PrincipalContext> contextCache;
      private StringDict<SecurityAccount> accountCache;
      private Object _lock;

      public SecurityCache(Func<SecurityCache, IdentityReference, SecurityAccount> factory=null)
      {
         this.factory = (factory != null) ? factory : SecurityAccount.FactoryImpl;
         contextCache = new StringDict<PrincipalContext>();
         accountCache = new StringDict<SecurityAccount>();
         _lock = new object();
      }

      public PrincipalContext GetPricipalContext(String name, params ContextType[] ctx)
      {
         if (name==null) return null;

         PrincipalContext ret = null;
         lock (_lock)
         {
            if (contextCache.TryGetValue (name, out ret)) return ret;
         }

         PrincipalContext created = createPricipalContext(name, ContextType.Machine, ContextType.Domain, ContextType.ApplicationDirectory);
         lock (_lock)
         {
            if (contextCache.TryGetValue(name, out ret))
            {
               Utils.FreeAndNil(ref created);
               return ret;
            }
            contextCache.Add(name, created);
            return created;
         }
      }

      public SecurityAccount GetAccount(String name)
      {
         SecurityAccount ret = null;
         lock (_lock)
         {
            if (accountCache.TryGetValue(name, out ret)) return ret;
         }
         return null;
      }

      public SecurityAccount GetAccount(IdentityReference ident)
      {
         SecurityAccount ret = null;
         lock (_lock)
         {
            if (accountCache.TryGetValue(ident.Value, out ret))
            {
               Utils.Free (ident);
               return ret;
            }
         }

         SecurityAccount created = factory (this, ident);
         lock (_lock)
         {
            if (accountCache.TryGetValue(ident.Value, out ret))
            {
               Utils.Free(ident);
               Utils.Free(created);
               return ret;
            }
            accountList = null;
            accountCache.Add(created.NTAccount.Value, created);
            accountCache.Add(created.Sid.Value, created);
         }
         return created;
      }

      public List<SecurityAccount> GetAccountList()
      {
         lock (_lock)
         {
            if (this.accountList != null) return this.accountList;

            List<SecurityAccount> tmp = new List<SecurityAccount>();
            foreach (var kvp in accountCache)
            {
               tmp.Add(kvp.Value);
            }
            accountList = tmp;
            return tmp;
         }
      }

      public static String CtxToString (PrincipalContext ctx)
      {
         if (ctx==null) return "PrincipalContext[null]";
         return String.Format("PrincipalContext['{2}' type={0}, options={1}, container={3}, server={4}]", ctx.ContextType, ctx.Options, ctx.Name, ctx.Container, "?");//ctx.ConnectedServer);
      }

      
      private PrincipalContext createPricipalContext(String name, params ContextType[] ctx)
      {
         for (int i = 0; i < ctx.Length; i++)
         {
            try
            {
               return new PrincipalContext(ctx[i], name);
            }
            catch (Exception err) { }
         }
         return null;
      }


      public virtual void Dispose()
      {
         clearDict(ref contextCache);
         clearDict(ref accountCache);
      }

      private void clearDict<T>(ref StringDict<T> refDict) where T : class
      {
         StringDict<T> dict = refDict;
         refDict = null;
         Exception e = null;
         foreach (var kvp in dict)
         {
            Utils.Free(kvp.Value, ref e);
         }
      }
   }
}
