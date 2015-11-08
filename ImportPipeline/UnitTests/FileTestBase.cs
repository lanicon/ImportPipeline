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

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Bitmanager.ImportPipeline.Template;
using System.Reflection;
using System.IO;
using System.Text;
using Bitmanager.Core;
using Bitmanager.ImportPipeline;
using Bitmanager.IO;

namespace UnitTests
{
   public abstract class FileTestBase
   {
      protected readonly String root;
      protected readonly String oldDataRoot, newDataRoot;
      protected FileTestBase(String dirName=null)
      {
         if (dirName == null) dirName = guessName();
         root = IOUtils.FindDirectoryToRoot(Path.GetDirectoryName(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)), "data");
         root = String.Format(@"{0}\{1}\", root, dirName);
         oldDataRoot = root + "old\\";
         newDataRoot = root + "new\\";
         IOUtils.ForceDirectories(newDataRoot, true);
         IOUtils.ForceDirectories(oldDataRoot, true);
         guessName();
      }

      private String guessName ()
      {
         String x = GetType().Name;
         if (x.EndsWith("tests", StringComparison.OrdinalIgnoreCase))
            return x.Substring(0, x.Length - 5);
         if (x.EndsWith("test", StringComparison.OrdinalIgnoreCase))
            return x.Substring(0, x.Length - 4);
         return x;
      }


      protected void CheckFiles (String name)
      {
         String actual = IOUtils.LoadFromFile(newDataRoot + name);
         String expectedFn = oldDataRoot + name;
         if (!File.Exists(expectedFn))
            Assert.Fail("Old version of [{0}] does not exist.", name);

         String expected = IOUtils.LoadFromFile(expectedFn);
         if (actual != expected)
            Assert.Fail("Old/new files for [{0}] are different.", name);
      }
   }
}
