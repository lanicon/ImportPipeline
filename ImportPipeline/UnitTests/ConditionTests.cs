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
using Bitmanager.ImportPipeline.Conditions;
using Newtonsoft.Json.Linq;

namespace UnitTests
{
   [TestClass]
   public class ConditionTests
   {
      [TestMethod]
      public void TestMethod1()
      {
         Condition c = Condition.Create(",string|lt,b");
         Assert.AreEqual(true, c.HasCondition((JToken)null));
         Assert.AreEqual(true, c.HasCondition((JToken)"a"));
         Assert.AreEqual(false, c.HasCondition((JToken)"b"));
         Assert.AreEqual(false, c.HasCondition((JToken)"c"));

         c = Condition.Create(",string|gt,b");
         Assert.AreEqual(false, c.HasCondition((JToken)"A"));
         Assert.AreEqual(false, c.HasCondition((JToken)"B"));
         Assert.AreEqual(true, c.HasCondition((JToken)"C"));

         c = Condition.Create(",string|gt|casesensitive,b");
         Assert.AreEqual(false, c.HasCondition((JToken)"A"));
         Assert.AreEqual(false, c.HasCondition((JToken)"B"));
         Assert.AreEqual(false, c.HasCondition((JToken)"C"));

         Assert.AreEqual("NullOrEmptyCondition only allows EQ-operator.", shouldFail(",string|lt,"));

         c = Condition.Create(",string|,");
         Assert.AreEqual(true, c.HasCondition((JToken)null));
         Assert.AreEqual(true, c.HasCondition((JToken)""));
         Assert.AreEqual(false, c.HasCondition((JToken)"C"));

         c = Condition.Create(",double|,1.0");
         Assert.AreEqual(true, c.HasCondition((JToken)1));
         Assert.AreEqual(true, c.HasCondition((JToken)1.0));
         Assert.AreEqual(false, c.HasCondition((JToken)2));

         c = Condition.Create(",double|gt,1.0");
         Assert.AreEqual(false, c.HasCondition((JToken)1));
         Assert.AreEqual(true, c.HasCondition((JToken)2));
         Assert.AreEqual(false, c.HasCondition((JToken)0.9));

         c = Condition.Create(",int|,1");
         Assert.AreEqual(true, c.HasCondition((JToken)1));
         Assert.AreEqual(true, c.HasCondition((JToken)1.0));
         Assert.AreEqual(false, c.HasCondition((JToken)2));

         c = Condition.Create(",int|gt,1");
         Assert.AreEqual(false, c.HasCondition((JToken)1));
         Assert.AreEqual(true, c.HasCondition((JToken)2));
         Assert.AreEqual(false, c.HasCondition((JToken)0.9));
      }

      private String shouldFail (String cond)
      {
         try
         {
            Condition.Create(cond);
            return null;
         }
         catch (Exception e)
         {
            return e.Message;
         }
      }
   }
}
