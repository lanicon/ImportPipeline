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
using Bitmanager.ImportPipeline;
using System.Reflection;
using System.IO;
using Bitmanager.Json;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace UnitTests
{
   [TestClass]
   public class FileBasedMapReduceWritersTests
   {
      String dir;
      JComparer hasher0, hasher1, hasher2, hasher3;
      JComparer cmp0, cmp1, cmp2, cmp3;
      public FileBasedMapReduceWritersTests()
      {
         dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
         var keyAndTypes = new List<KeyAndType>();
         hasher0 = JComparer.Create(keyAndTypes);
         cmp0 = JComparer.Create(keyAndTypes);

         keyAndTypes.Add (new KeyAndType ("k1", CompareType.Int));
         hasher1 = JComparer.Create(keyAndTypes);
         cmp1 = JComparer.Create(keyAndTypes);

         keyAndTypes.Add(new KeyAndType("k2", CompareType.String));
         hasher2 = JComparer.Create(keyAndTypes);
         cmp2 = JComparer.Create(keyAndTypes);

         keyAndTypes.Add(new KeyAndType("k3", CompareType.Double));
         hasher3 = JComparer.Create(keyAndTypes);
         cmp3 = JComparer.Create(keyAndTypes);

         Console.WriteLine("Hasher0={0}, cmp0={1}", hasher0, cmp0);
         Console.WriteLine("Hasher1={0}, cmp1={1}", hasher1, cmp1);
         Console.WriteLine("Hasher2={0}, cmp2={1}", hasher2, cmp2);
         Console.WriteLine("Hasher3={0}, cmp3={1}", hasher3, cmp3);
      }
      [TestMethod]
      public void TestCmpNull()
      {
         var cmp = JComparer.Create(new List<KeyAndType> { new KeyAndType("k1", CompareType.Int) });
         var cmpRev = JComparer.Create(new List<KeyAndType> { new KeyAndType("k1", CompareType.Int | CompareType.Descending) });// { CompareType.Int | CompareType.Descending });
         JObject o1 = Create(1, "22", 23);
         JObject o2 = Create("22", 23);
         JObject o3 = Create(2, "22");
         checkCompare(o1, o3, cmp, -1);
         checkCompare(o2, o3, cmp, -1);
         checkCompare(o2, o2, cmp, 0);
         checkCompare(o3, o2, cmp, 1);

         checkCompare(o2, o2, cmpRev, 0);
         checkCompare(o3, o2, cmpRev, -1);
      }

      [TestMethod]
      public void TestCmpString()
      {
         //var cmp = JComparer.Create(new List<JPath> { new JPath("k2") }, new List<CompareType>() { CompareType.String });
         //var cmpI = JComparer.Create(new List<JPath> { new JPath("k2") }, new List<CompareType>() { CompareType.String | CompareType.CaseInsensitive });
         //var cmpRev = JComparer.Create(new List<JPath> { new JPath("k2") }, new List<CompareType>() { CompareType.String | CompareType.Descending });
         var cmp = JComparer.Create(new List<KeyAndType> { new KeyAndType("k2", CompareType.String) });
         var cmpI = JComparer.Create(new List<KeyAndType> { new KeyAndType("k2", CompareType.String | CompareType.CaseInsensitive) });
         var cmpRev = JComparer.Create(new List<KeyAndType> { new KeyAndType("k2", CompareType.String | CompareType.Descending) });
         JObject o1 = Create(1, "aB", 23);
         JObject o2 = Create("AB", 23);
         JObject o3 = Create(2, "bb");
         JObject o4 = Create(2, "");
         JObject o5 = Create(2, null);

         checkCompare(o1, o1, cmp, 0);
         checkCompare(o1, o1, cmpI, 0);
         checkCompare(o1, o1, cmpRev, 0);

         checkCompare(o4, o5, cmp, 0);
         checkCompare(o4, o5, cmpI, 0);
         checkCompare(o4, o5, cmpRev, 0);

         checkCompare(o1, o3, cmp, -1);
         checkCompare(o1, o2, cmp, 1);
         checkCompare(o1, o2, cmpI, 0);
         checkCompare(o1, o2, cmpRev, -1);

         var list = new List<JObject>();
         list.Add(o1);
         list.Add(o2);
         list.Add(o3);
         list.Add(o4);
         list.Add(o5);
         list.Add(Create("BB", 23));
         list.Add(Create("Bb", 23));
         list.Add(Create("bB", 23));
         list.Add(Create("c", 23));
         list.Add(Create("C", 23));

         sortAndDump(list, cmp, "Case-sens");
         sortAndDump(list, cmpI, "Case-in-sens");
      }

      private List<JObject> sortAndDump(List<JObject> list, JComparer cmp, string p)
      {
         Console.WriteLine("Dumping entries after sorting {0}...", p);
         list.Sort(cmp);
         foreach (var o in list)
            Console.WriteLine("-- " + o.ToString(Newtonsoft.Json.Formatting.None));
         return list;
      }
      private List<JObject> dump(List<JObject> list, string p)
      {
         Console.WriteLine("Dumping entries. {0}.", p);
         foreach (var o in list)
            Console.WriteLine("-- " + o.ToString(Newtonsoft.Json.Formatting.None));
         return list;
      }
      [TestMethod]
      public void TestHashes()
      {
         JObject o1 = Create(1, "22", 23);
         JObject o2 = Create("22", 23);
         JObject o3 = Create(1, "22");
         JObject o4 = Create(1, null, 23);
         JObject o5 = Create(1, "", 23);
         checkHash(o1, hasher0, 0, -1);
         checkHash(o1, hasher1, 1, -1);
         checkHash(o1, hasher2, -843532401, -1);
         checkHash(o1, hasher3, -1919961201, -1);

         checkHash(o2, hasher0, 0, -1);
         checkHash(o2, hasher1, 0, 0);
         checkHash(o2, hasher2, -843532402, 0);
         checkHash(o2, hasher3, -1919961202, 0);

         checkHash(o3, hasher0, 0, -1);
         checkHash(o3, hasher1, 1, -1);
         checkHash(o3, hasher2, -843532401, -1);
         checkHash(o3, hasher3, -843532401, 2);

         checkHash(o4, hasher0, 0, -1);
         checkHash(o4, hasher1, 1, -1);
         checkHash(o4, hasher2, 1, 1);
         checkHash(o4, hasher3, 1077346305, 1);

         checkHash(o5, hasher0, 0, -1);
         checkHash(o5, hasher1, 1, -1);
         checkHash(o5, hasher2, 1, 1);
         checkHash(o5, hasher3, 1077346305, 1);

         checkCompare(o1, o1, cmp1, 0);
         checkCompare(o1, o1, cmp2, 0);
         checkCompare(o1, o1, cmp3, 0);

         checkCompare(o4, o5, cmp1, 0);
         checkCompare(o4, o5, cmp2, 0);
         checkCompare(o4, o5, cmp3, 0);

         checkCompare(o1, o4, cmp1, 0);
         checkCompare(o1, o4, cmp2, 1);
         checkCompare(o1, o4, cmp3, 1);

         checkCompare(o1, o2, cmp1, 1);
         checkCompare(o1, o2, cmp2, 1);
         checkCompare(o1, o2, cmp3, 1);
      }

      private static String _tos (JObject obj)
      {
         if (obj == null) return "NULL";
         String tmp = obj.ToString(Newtonsoft.Json.Formatting.None);
         return tmp.Replace ('\"', '\'');
      }
      [TestMethod]
      public void TestMapReduce()
      {
         var list = new List<JObject>();
         list.Add(Create(1, "22", 23));
         list.Add(Create("22", 23));
         list.Add(Create(1, "22"));
         list.Add(Create(1, null, 23));
         list.Add(Create(1, "", 23));
         list.Add(Create("BB", 23));
         list.Add(Create("Bb", 23));
         list.Add(Create("bB", 23));
         list.Add(Create("c", 23));
         list.Add(Create("C", 23));

         var wtrs = new FileBasedMapperWriters(hasher3, cmp3, Path.Combine(dir, "data"), "foobar", 1, true, false);
         var outList = dump(mapReduce(wtrs, list), "1 file");
         wtrs.Dispose();
         Assert.AreEqual("{'k2':'22','k3':23}", _tos(outList[0]));
         Assert.AreEqual("{'k1':1,'k2':'22','k3':23}", _tos(outList[9]));

         wtrs = new FileBasedMapperWriters(hasher3, cmp3, Path.Combine(dir, "data"), "foobar", 3, true, false);
         outList = dump(mapReduce(wtrs, list), "3 files");
         Assert.AreEqual("{'k2':'22','k3':23}", _tos(outList[0]));
         Assert.AreEqual("{'k2':'bB','k3':23}", _tos(outList[9]));
         wtrs.Dispose(); 


         wtrs = new FileBasedMapperWriters(hasher3, cmp3, Path.Combine(dir, "data"), "foobar", 1, false, false);
         outList = dump(mapReduce(wtrs, list), "1 file");
         wtrs.Dispose();
         Assert.AreEqual("{'k2':'22','k3':23}", _tos(outList[0]));
         Assert.AreEqual("{'k1':1,'k2':'22','k3':23}", _tos(outList[9]));

         wtrs = new FileBasedMapperWriters(hasher3, cmp3, Path.Combine(dir, "data"), "foobar", 3, false, false);
         outList = dump(mapReduce(wtrs, list), "1 file");
         wtrs.Dispose();
         Assert.AreEqual("{'k2':'22','k3':23}", _tos(outList[0]));
         Assert.AreEqual("{'k2':'bB','k3':23}", _tos(outList[9]));
      }

      List<JObject> mapReduce (FileBasedMapperWriters wtrs, List<JObject> list)
      {
         var ret = new List<JObject>();
         foreach (var o in list) wtrs.Write(o);

         foreach (var o in wtrs) ret.Add(o);

         Assert.AreEqual(list.Count, ret.Count);
         return ret;
      }

      private static void checkHash(JObject o, JComparer hasher, int expHash, int expNullIndex)
      {
         int nullIndex;
         int h = hasher.GetHash(o, out nullIndex);
         Assert.AreEqual(expHash, h);
         Assert.AreEqual(expHash, hasher.GetHash(o));
         Assert.AreEqual(expNullIndex, nullIndex);
      }
      private static void checkCompare(JObject a, JObject b, JComparer cmp, int exp)
      {
         int rc = cmp.Compare(a, b);
         if (rc < 0) rc = -1;
         else if (rc > 0) rc = 1;

         Assert.AreEqual(exp, rc);
      }

      private JObject Create(int k1, String k2, int k3)
      {
         JObject ret = new JObject();
         ret["k1"] = (JToken)k1;
         ret["k2"] = (JToken)k2;
         ret["k3"] = (JToken)k3;
         return ret;
      }
      private JObject Create(String k2, int k3)
      {
         JObject ret = new JObject();
         ret["k2"] = (JToken)k2;
         ret["k3"] = (JToken)k3;
         return ret;
      }
      private JObject Create(int k1, String k2)
      {
         JObject ret = new JObject();
         ret["k1"] = (JToken)k1;
         ret["k2"] = (JToken)k2;
         return ret;
      }
   }
}
