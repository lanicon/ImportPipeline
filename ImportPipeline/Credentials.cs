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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Bitmanager.Importer
{
   public class CredentialsHelper
   {
      //static Logger logger = Logs.CreateLogger("cred", "cred");
      /// <summary>
      /// Prompts for password.
      /// </summary>
      /// <param name="user">The user.</param>
      /// <param name="password">The password.</param>
      /// <returns>True if no errors.</returns>
      public static bool PromptForCredentials (String caption, String msg, ref string user, out string password)
      {
         // Setup the flags and variables
         StringBuilder passwordBuf = new StringBuilder(), userBuf = new StringBuilder();
         CREDUI_INFO credUI = new CREDUI_INFO();
         credUI.pszCaptionText = !String.IsNullOrEmpty(caption) ? caption : "Please enter username/password";
         credUI.pszMessageText = msg;
         userBuf.Append(user);
         credUI.cbSize = Marshal.SizeOf(credUI);
         bool save = false;
         CREDUI_FLAGS flags = CREDUI_FLAGS.ALWAYS_SHOW_UI | CREDUI_FLAGS.GENERIC_CREDENTIALS | CREDUI_FLAGS.DO_NOT_PERSIST;// | CREDUI_FLAGS.KEEP_USERNAME;

         // Prompt the user
         CredUIReturnCodes returnCode = CredUIPromptForCredentialsW(ref credUI, "XX", IntPtr.Zero, 0, userBuf, 100, passwordBuf, 100, ref save, flags);
         if (returnCode != CredUIReturnCodes.NO_ERROR)
         {
            Logger err = Logs.ErrorLog;
            err.Log("PromptForCredentials failed: {0}", returnCode);
            err.Log("-- Caption={0}, msg={1}", caption, msg);
            password = null;
            return false;
         }
         user = userBuf.ToString();
         password = passwordBuf.ToString();
         return true;
      }
      //public static bool PromptForPassword2(out string user, out string password)
      //{
      //   // Setup the flags and variables
      //   StringBuilder userPassword = new StringBuilder(), userID = new StringBuilder();
      //   CREDUI_INFO credUI = new CREDUI_INFO();
      //   credUI.pszCaptionText = "Please enter password";
      //   credUI.pszMessageText = "http://localhost/";
      //   userID.Append("user");
      //   credUI.cbSize = Marshal.SizeOf(credUI);
      //   bool save = false;
      //   CREDUI_FLAGS flags = CREDUI_FLAGS.ALWAYS_SHOW_UI | CREDUI_FLAGS.GENERIC_CREDENTIALS | CREDUI_FLAGS.DO_NOT_PERSIST;// | CREDUI_FLAGS.KEEP_USERNAME;

      //   IntPtr p1, p2;
      //   int errorCode=0;
      //   uint authPackage=0;
      //   // Prompt the user
      //   CredUIReturnCodes returnCode = CredUIPromptForWindowsCredentials(ref credUI, IntPtr.Zero, 0, userID, 100, userPassword, 100, ref save, 0);
      //   user = userID.ToString();
      //   password = userPassword.ToString();
      //   logger.Log("ret={0}, u={1}, p={2}", returnCode, user, password);

      //   return (returnCode == CredUIReturnCodes.NO_ERROR);
      //}

      [Flags]
      enum CREDUI_FLAGS
      {
         INCORRECT_PASSWORD = 0x1,
         DO_NOT_PERSIST = 0x2,
         REQUEST_ADMINISTRATOR = 0x4,
         EXCLUDE_CERTIFICATES = 0x8,
         REQUIRE_CERTIFICATE = 0x10,
         SHOW_SAVE_CHECK_BOX = 0x40,
         ALWAYS_SHOW_UI = 0x80,
         REQUIRE_SMARTCARD = 0x100,
         PASSWORD_ONLY_OK = 0x200,
         VALIDATE_USERNAME = 0x400,
         COMPLETE_USERNAME = 0x800,
         PERSIST = 0x1000,
         SERVER_CREDENTIAL = 0x4000,
         EXPECT_CONFIRMATION = 0x20000,
         GENERIC_CREDENTIALS = 0x40000,
         USERNAME_TARGET_CREDENTIALS = 0x80000,
         KEEP_USERNAME = 0x100000,
      }

      public enum CredUIReturnCodes
      {
         NO_ERROR = 0,
         ERROR_CANCELLED = 1223,
         ERROR_NO_SUCH_LOGON_SESSION = 1312,
         ERROR_NOT_FOUND = 1168,
         ERROR_INVALID_ACCOUNT_NAME = 1315,
         ERROR_INSUFFICIENT_BUFFER = 122,
         ERROR_INVALID_PARAMETER = 87,
         ERROR_INVALID_FLAGS = 1004,
      }
      [DllImport("credui", CharSet = CharSet.Unicode)]
      private static extern CredUIReturnCodes CredUIPromptForCredentialsW(ref CREDUI_INFO creditUR,
        string targetName,
        IntPtr reserved1,
        int iError,
        StringBuilder userName,
        int maxUserName,
        StringBuilder password,
        int maxPassword,
        [MarshalAs(UnmanagedType.Bool)] ref bool pfSave,
        CREDUI_FLAGS flags);
   
      [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
      public struct CREDUI_INFO
      {
         public int cbSize;
         public IntPtr hwndParent;
         public string pszMessageText;
         public string pszCaptionText;
         public IntPtr hbmBanner;
      }

      [DllImport("credui.dll", CharSet = CharSet.Unicode)]
      private static extern uint CredUIPromptForWindowsCredentials(ref CREDUI_INFO notUsedHere,
        int authError,
        ref uint authPackage,
        IntPtr InAuthBuffer,
        uint InAuthBufferSize,
        out IntPtr refOutAuthBuffer,
        out uint refOutAuthBufferSize,
        ref bool fSave,
        PromptForWindowsCredentialsFlags flags);

      private enum PromptForWindowsCredentialsFlags
      {
         /// <summary>
         /// The caller is requesting that the credential provider return the user name and password in plain text.
         /// This value cannot be combined with SECURE_PROMPT.
         /// </summary>
         CREDUIWIN_GENERIC = 0x1,
         /// <summary>
         /// The Save check box is displayed in the dialog box.
         /// </summary>
         CREDUIWIN_CHECKBOX = 0x2,
         /// <summary>
         /// Only credential providers that support the authentication package specified by the authPackage parameter should be enumerated.
         /// This value cannot be combined with CREDUIWIN_IN_CRED_ONLY.
         /// </summary>
         CREDUIWIN_AUTHPACKAGE_ONLY = 0x10,
         /// <summary>
         /// Only the credentials specified by the InAuthBuffer parameter for the authentication package specified by the authPackage parameter should be enumerated.
         /// If this flag is set, and the InAuthBuffer parameter is NULL, the function fails.
         /// This value cannot be combined with CREDUIWIN_AUTHPACKAGE_ONLY.
         /// </summary>
         CREDUIWIN_IN_CRED_ONLY = 0x20,
         /// <summary>
         /// Credential providers should enumerate only administrators. This value is intended for User Account Control (UAC) purposes only. We recommend that external callers not set this flag.
         /// </summary>
         CREDUIWIN_ENUMERATE_ADMINS = 0x100,
         /// <summary>
         /// Only the incoming credentials for the authentication package specified by the authPackage parameter should be enumerated.
         /// </summary>
         CREDUIWIN_ENUMERATE_CURRENT_USER = 0x200,
         /// <summary>
         /// The credential dialog box should be displayed on the secure desktop. This value cannot be combined with CREDUIWIN_GENERIC.
         /// Windows Vista: This value is not supported until Windows Vista with SP1.
         /// </summary>
         CREDUIWIN_SECURE_PROMPT = 0x1000,
         /// <summary>
         /// The credential provider should align the credential BLOB pointed to by the refOutAuthBuffer parameter to a 32-bit boundary, even if the provider is running on a 64-bit system.
         /// </summary>
         CREDUIWIN_PACK_32_WOW = 0x10000000,
      }

   }
}
