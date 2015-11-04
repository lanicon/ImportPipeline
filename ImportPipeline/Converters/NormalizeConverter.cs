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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Bitmanager.Core;
using Bitmanager.Xml;
using Newtonsoft.Json.Linq;
using Bitmanager.Json;


namespace Bitmanager.ImportPipeline
{
   public class NormalizeConverter : Converter
   {

      public NormalizeConverter(XmlNode node)
         : base(node)
      {
      }

      public override Object ConvertScalar(PipelineContext ctx, Object obj)
      {
         if (obj == null) return null;
         String x = obj.ToString();
         if (String.IsNullOrEmpty(x)) return x;

         String norm = x.Normalize(NormalizationForm.FormD);
         int i;
         for (i=0; i<norm.Length; i++)
         {
            var cat = char.GetUnicodeCategory (norm[i]);
            if (cat == System.Globalization.UnicodeCategory.NonSpacingMark) goto REMOVE;
         }
         return x;

         REMOVE:
         StringBuilder buf = new StringBuilder(norm.Length);
         for (int j = 0; j < i; j++) buf.Append(norm[j]);

         for (++i; i<norm.Length; i++)
         {
            var cat = char.GetUnicodeCategory(norm[i]);
            if (cat == System.Globalization.UnicodeCategory.NonSpacingMark) continue;
            buf.Append(norm[i]);
         }
         return buf.ToString().Normalize(NormalizationForm.FormC);
      }




   }
}
