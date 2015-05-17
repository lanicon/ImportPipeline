using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Globalization;
using System.Web;
using System.Reflection;
using Bitmanager.ImportPipeline.StreamProviders;

namespace Bitmanager.ImportPipeline
{
   /// <summary>
   /// Tries to select a subfield of an object. Currently only JToken and IStreamProvider are supported
   /// </summary>
   public class Selector : Converter
   {
      private enum ProviderField { FullName, VirtualName, RelativeName, LastModified, Uri, None };
      private static Dictionary<Type, String> selectorDict;
      private Dictionary<Type, Type> typeFilter;
      private String field;
      private JPath jsonExpr;
      private JEvaluateFlags jsonFlags;
      private ProviderField providerField;
      private bool skipNoMatch;

      public Selector(XmlNode node, String type)
         : base(node)
      {
         skipNoMatch = node.ReadBool("skipnomatch", true);
         field = node.ReadStr("@field");
         providerField = ProviderField.None;
         switch (field.ToLowerInvariant())
         {
            case "VirtualName": providerField = ProviderField.VirtualName; break;
            case "RelativeName": providerField = ProviderField.RelativeName; break;
            case "Name": providerField = ProviderField.FullName; break;
            case "FullName": providerField = ProviderField.FullName; break;
            case "Uri": providerField = ProviderField.Uri; break;
            case "LastModified": providerField = ProviderField.LastModified; break;
         }
         jsonExpr = new JPath(field);
         jsonFlags = node.ReadEnum("@flags", JEvaluateFlags.NoExceptMissing | JEvaluateFlags.NoExceptWrongType);
      }

      public Selector(String name, String expr, bool skipNoMatch, JEvaluateFlags flags)
         : base(name)
      {
         this.skipNoMatch = skipNoMatch;
         providerField = ProviderField.None;
         switch (expr.ToLowerInvariant())
         {
            case "virtualname": providerField = ProviderField.VirtualName; break;
            case "relativename": providerField = ProviderField.RelativeName; break;
            case "name": providerField = ProviderField.FullName; break;
            case "fullname": providerField = ProviderField.FullName; break;
            case "uri": providerField = ProviderField.Uri; break;
            case "lastmodified": providerField = ProviderField.LastModified; break;
         }
         jsonExpr = new JPath(expr);
         jsonFlags = flags;
      }

      public override object ConvertScalar(PipelineContext ctx, object obj)
      {
         if (obj == null) return null;
         var p = obj as IStreamProvider;
         if (p != null)
         {
            switch (providerField)
            {
               default: break;
               case ProviderField.FullName: return p.FullName;
               case ProviderField.VirtualName: return p.VirtualName;
               case ProviderField.RelativeName: return p.RelativeName;
               case ProviderField.LastModified: return p.LastModified;
               case ProviderField.Uri: return p.Uri;
            }
         }

         var jt = obj as JToken;
         if (jt != null)
         {
            return jsonExpr.Evaluate(jt, jsonFlags);
         }

         return skipNoMatch ? null : obj; 
      }


   }
}
