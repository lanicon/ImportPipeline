using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmanager.ImportPipeline.Template
{
   public interface ITemplateSettings
   {
      IVariables InitialVariables { get; set; }
      int DebugLevel { get; set; }
      bool AutoWriteGenerated { get; set; }
      ITemplateSettings Clone();
   }

   public class TemplateSettings : ITemplateSettings
   {
      public virtual IVariables InitialVariables { get; set; }
      public virtual int DebugLevel { get; set; }
      public virtual bool AutoWriteGenerated { get; set; }
      public virtual ITemplateSettings Clone() { return new TemplateSettings(this); }

      public TemplateSettings() { }
      public TemplateSettings(bool dump) { AutoWriteGenerated = dump; }
      public TemplateSettings(int dbgLevel) { DebugLevel = dbgLevel; }
      public TemplateSettings(bool dump, int dbgLevel) { AutoWriteGenerated = dump; DebugLevel = dbgLevel; }
      public TemplateSettings(TemplateSettings other)
      {
         InitialVariables = other.InitialVariables;
         DebugLevel = other.DebugLevel;
         AutoWriteGenerated = other.AutoWriteGenerated;
      }

   }
}
