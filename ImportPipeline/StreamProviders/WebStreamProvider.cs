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
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using Bitmanager.Xml;
using Bitmanager.Core;
using Bitmanager.IO;
using Bitmanager.Importer;

namespace Bitmanager.ImportPipeline.StreamProviders
{
   public class WebStreamProvider : StreamProvider
   {
      public bool KeepAlive;

      public WebStreamProvider(PipelineContext ctx, XmlNode node, XmlNode parentNode, StreamDirectory parent)
         : base(parent, node)
      {
         if (parentNode == null) parentNode = node;
         silent = (ctx.ImportFlags & _ImportFlags.Silent) != 0;
         String root = parentNode.ReadStr("@root", null);
         String url = node.ReadStr("@url");
         uri = root == null ? new Uri(url) : new Uri(new Uri(root), url);
         fullName = uri.ToString();
         KeepAlive = node.ReadBool(1, "@keepalive", true);
      }
      public WebStreamProvider(PipelineContext ctx, StreamProvider other, String url)
         : base(other)
      {
         uri = new Uri(url);
         fullName = uri.ToString();
      }

      protected virtual void onPrepareRequest(HttpWebRequest req)
      {
         PrepareRequest(req);
      }
      public virtual void PrepareRequest(HttpWebRequest req)
      {
         req.KeepAlive = KeepAlive;
         req.Credentials = Credentials;
      }

      public override Stream CreateStream()
      {
         HttpWebRequest req = (HttpWebRequest)HttpWebRequest.Create(uri);
         PrepareRequest(req);
         //if (Timeout > 0 || Timeout == -1)
         //{
         //   req.Timeout = Timeout;
         //   req.ReadWriteTimeout = Timeout;
         //}
         HttpWebResponse resp;
         try
         {
            resp = (HttpWebResponse)req.GetResponse();
         }
         catch (WebException we)
         {
            throw new BMWebException(we, uri);
         }
         return new WebStreamWrapper(resp);
      }


      class WebStreamWrapper : StreamWrapperBase
      {
         HttpWebResponse resp;
         public WebStreamWrapper(HttpWebResponse resp)
            : base(resp.GetResponseStream())
         {
            this.resp = resp;
         }

         protected override void Dispose(bool disposing)
         {
            base.Dispose(disposing);
            Utils.FreeAndNil(ref resp);
         }
      }

      public class BMWebException : BMException
      {
         public readonly Uri Uri;
         public readonly String ResponseText;
         public BMWebException(WebException ex, Uri uri)
            : base(ex, createMsg(ex, uri))
         {
            using (Stream strm = ex.Response.GetResponseStream())
            {
               ResponseText = Encoding.UTF8.GetString(strm.ReadAllBytes());
            }
            Logs.ErrorLog.Log(_LogType.ltError, Message);
            Logs.ErrorLog.Log(_LogType.ltError, "Response status={0}, Response text={1}", ex.Status, ResponseText);
         }

         private static String createMsg(WebException ex, Uri uri)
         {
            String msg = ex.Message;
            String url = uri.ToString();
            if (msg.IndexOf(url) < 0) msg = msg + "\r\nUrl=" + url + ".";
            return msg;
         }

      }
   }
}
