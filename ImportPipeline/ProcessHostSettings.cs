/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Xml;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;


namespace Bitmanager.Java
{
   public class ProcessHostSettings
   {
      public readonly String ErrorLogName;
      public readonly String LogName;
      public readonly String LogFrom;
      public readonly String ShutdownUrl;
      public readonly String ShutdownMethod;
      public readonly String ExeName;
      public readonly String Arguments;
      public readonly int StartDelay; 
      public readonly int MaxRestarts;
      public readonly bool ClearLogs;

      public ProcessHostSettings(XmlNode node)
      {
         MaxRestarts = node.ReadInt("@restarts", 25);
         ClearLogs = node.ReadBool("@clearlogs", true);
         LogName = node.ReadStr("@log", "console");
         ErrorLogName = node.ReadStr("@errlog", LogName);
         LogFrom = node.ReadStr("@logfrom", "console");
         ExeName = node.ReadStr("exe");
         Arguments = node.ReadStr("arguments", null);
         StartDelay = node.ReadInt("@startdelay", -1);

         ShutdownUrl = node.ReadStr("shutdown/@url", null);
         if (ShutdownUrl != null) ShutdownMethod = node.ReadStr("shutdown/@method", "POST");
      }

   }

   
   
   
   
   //public class Settings
   //{
   //   const String Version = "1.0";

   //   public enum _VarType { Default, Override };
   //   public readonly XmlHelper Xml;
   //   public readonly List<ProcessSettings> Processes;
   //   public readonly String ServiceName;
   //   public bool IsValid { get { return Xml != null; } }

   //   public Settings(String defServiceName, String fn=null, bool optional=false)
   //   {
   //      ServiceName = defServiceName;

   //      String dir = Path.GetDirectoryName (Assembly.GetExecutingAssembly ().Location);
   //      String fullName = IOUtils.FindFileToRoot(dir, String.IsNullOrEmpty(fn) ? "settings.xml" : fn, FindToTootFlags.ReturnOriginal);

   //      if (optional && !File.Exists (fullName)) return;

   //      Xml = new XmlHelper(fullName);
   //      Xml.CheckVersion(Version);
   //      ServiceName = Xml.OptReadStr("service/@name", defServiceName);

   //      XmlNodeList processNodes = Xml.SelectNodes("service/process");
   //      Processes = new List<ProcessSettings>(processNodes.Count);
   //      for (int i=0; i<processNodes.Count; i++)
   //         Processes.Add (new ProcessSettings (this, processNodes[i]));


   //      XmlNodeList vars = Xml.SelectNodes("variables/variable");
   //      foreach (XmlNode v in vars)
   //      {
   //         String name = XmlUtils.ReadStr(v, "@name");
   //         String value = XmlUtils.OptReadStr(v, "@value", String.Empty);
   //         _VarType vt = XmlUtils.OptReadEnum (v, "@type", _VarType.Override);
   //         if (vt== _VarType.Default)
   //         {
   //            if (!String.IsNullOrEmpty (Environment.GetEnvironmentVariable (name))) 
   //               continue;
   //         }
   //         Environment.SetEnvironmentVariable (name, value);
   //      }

   //      //foreach (DictionaryEntry kvp in Environment.GetEnvironmentVariables())
   //      //{
   //      //   String name = (String)kvp.Key;
   //      //   if (Variables.ContainsKey(name)) continue;
   //      //   Variables.Add(name, Environment.ExpandEnvironmentVariables((String)kvp.Value));
   //      //}

   //   }
   //}
}
