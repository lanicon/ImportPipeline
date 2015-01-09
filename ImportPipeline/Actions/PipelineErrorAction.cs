using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bitmanager.Core;
using Bitmanager.Xml;
using System.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;
using System.Text.RegularExpressions;

namespace Bitmanager.ImportPipeline
{
   public class PipelineErrorAction : PipelineAction
   {
      public PipelineErrorAction(Pipeline pipeline, XmlNode node)
         : base(pipeline, node)
      {
      }

      internal PipelineErrorAction(PipelineAddAction template, String name, Regex regex)
         : base(template, name, regex)
      {
      }

      public override void Start(PipelineContext ctx)
      {
         base.Start(ctx);
         IErrorEndpoint ep = Endpoint as IErrorEndpoint;
         if (ep == null) throw new BMException("Endpoint does not support IErrorEndpoint. Action={0}", this);
      }

      public override Object HandleValue(PipelineContext ctx, String key, Object value)
      {
         value = ConvertAndCallScript(ctx, key, value);
         if ((ctx.ActionFlags & _ActionFlags.Skip) != 0) { ctx.Skipped++; goto EXIT_RTN; }

         Exception err = value as Exception;
         if (err == null)
         {
            try
            {
               String msg = value == null ? "null" : value.ToString();
               throw new BMException(msg);
            }
            catch (Exception e)
            {
               err = e;
            }
         }

         ((IErrorEndpoint)Endpoint).SaveError(ctx, err);
         endPoint.Clear();
         pipeline.ClearVariables();

      EXIT_RTN:
         return PostProcess(ctx, value);
      }
   }

}
