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
using System.Data.SqlClient;
using System.Data;
using MySql.Data.MySqlClient;
using System.Data.Common;
using System.Data.Odbc;

namespace Bitmanager.ImportPipeline
{
   public class SqlDatasource : Datasource
   {
      private StringDict<ConversionError> conversionErrors;
      public String ConnectionString;
      public List<Query> Queries;
      public int? ConnectionTimeout;
      public int? DefaultCommandTimeout;
      public bool AllowConversionErrors;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         ConnectionString = node.ReadStr("connection");
         ConnectionTimeout = ReadTimeoutFromNode (node, "connection/@timeout", null);
         int? DefaultCommandTimeout = ReadTimeoutFromNode(node, "connection/@query_timeout", 0);
         AllowConversionErrors = node.ReadBool("@allowconversionerrors", true);

         XmlNodeList ch = node.SelectNodes("query");
         if (ch.Count > 0)
         {
            Queries = new List<Query>(ch.Count);
            foreach (XmlNode n in ch) Queries.Add(new Query(n, DefaultCommandTimeout));
         }
      }

      protected String getConnectionStringPlusTimeout()
      {
         String tmp = ConnectionString;
         if (ConnectionTimeout == null || tmp==null) return tmp;
         for (int i = tmp.Length - 1; i >= 0; i--)
         {
            switch (tmp[i])
            {
               case ' ': continue;
               case ';': goto NO_APPEND;
            }
            break;
         }
         tmp = tmp + "; ";
         NO_APPEND:;
         return String.Format("{0} Connection Timeout={1};", tmp, (int)ConnectionTimeout);
      }

      public static int? ReadTimeoutFromNode (XmlNode node, String key, int? def)
      {
         String x = node.ReadStrRaw(key, _XmlRawMode.Trim);
         if (x == null) return def;
         if (x.Length == 0) return null;
         return (999 + Invariant.ToInterval(x)) / 1000;
      }

      private void addConversionError(String field, Exception e)
      {
         if (conversionErrors == null) conversionErrors = new StringDict<ConversionError>();

         ConversionError err;
         if (conversionErrors.TryGetValue (field, out err))
            err.Count++;
         else
            conversionErrors.Add (field, new ConversionError(field, e));
      }

      private void dumpConversionErrors(Logger lg)
      {
         if (conversionErrors == null) return;
         lg.Log (_LogType.ltWarning, "Conversion errors occurred. Showing only the 1st exception and the total # errors");
         foreach (var kvp in conversionErrors)
         {
            lg.Log (_LogType.ltWarning, "-- Field={0}, err.count={1}, err.message={2}.", kvp.Value.Field, kvp.Value.Count, kvp.Value.Error);
         }
      }

      protected virtual DbConnection createConnection()
      {
         return new SqlConnection(ConnectionString);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         DbConnection connection = null;
         try
         {
            connection = createConnection();
            ctx.DebugLog.Log("Open SQL connection with [{0}], timeout={1} (sec).", connection.ConnectionString, connection.ConnectionTimeout);
            connection.Open();

            if (Queries == null)
               EmitTables(ctx, connection);
            else
               foreach (Query q in Queries)
                  EmitQuery(ctx, connection, q);
            dumpConversionErrors(ctx.ImportLog);
         }
         finally
         {
            Utils.FreeAndNil(ref connection);
         }
      }

      protected void EmitTables(PipelineContext ctx, DbConnection connection)
      {
         var tables = new List<String>();
         var cmd = connection.CreateCommand();
         cmd.CommandText = "show tables";
         cmd.CommandType = CommandType.Text;
         using (DbDataReader rdr = executeReader(ctx, cmd))
         {
            while (rdr.Read())
            {
               if (rdr.FieldCount==0) continue;
               String v = rdr.GetValue(0) as String;
               if (String.IsNullOrEmpty(v)) continue;
               tables.Add (v);
            }
         }

         foreach (var t in tables) EmitTable (ctx, connection, t);
      }

      protected void EmitTable(PipelineContext ctx, DbConnection connection, String table)
      {
         Query q = new Query(table, String.Format("select * from {0};", table), this.DefaultCommandTimeout, this.AllowConversionErrors);
         EmitQuery(ctx, connection, q);
      }

      protected void EmitQuery(PipelineContext ctx, DbConnection connection, Query q)
      {
         var cmd = q.CreateCommand(connection);
         ctx.DebugLog.Log("Exec SQL command [{0}], timeout={1} (sec).", cmd.CommandText, cmd.CommandTimeout);

         ctx.SendItemStart (cmd);
         using (DbDataReader rdr = executeReader(ctx, cmd))
         {
            EmitRecords(ctx, rdr, q);
         }
         ctx.SendItemStop (cmd);
      }

      protected virtual DbDataReader executeReader (PipelineContext ctx, DbCommand cmd)
      {
         try
         {
            ctx.ImportLog.Log("Excecuting query: " + cmd.CommandText);
            DbDataReader rdr = cmd.ExecuteReader();
            //ctx.ImportLog.Log("-- affected row={0}, hasRows={1}", rdr.RecordsAffected, rdr.HasRows);
            return rdr;
         }
         catch (Exception e)
         {
            ctx.ErrorLog.Log("SQL error on query");
            ctx.ErrorLog.Log(cmd.CommandText);
            ctx.ErrorLog.Log(e);
            throw;
         }
      }

      protected void EmitRecords(PipelineContext ctx, DbDataReader rdr, Query q)
      {
         String pfxRecord = String.IsNullOrEmpty(q.Prefix) ? "record" : q.Prefix;
         String pfxField = pfxRecord + "/";
         var sink = ctx.Pipeline;
         while (rdr.Read())
         {
            int fcnt = rdr.FieldCount;
            for (int i = 0; i < fcnt; i++)
            {
               String name = rdr.GetName(i);
               Type ft = rdr.GetFieldType(i);
               Object value = null;
               try
               {
                  if (!rdr.IsDBNull(i))
                     value = rdr.GetValue(i);
               } catch (Exception e)
               {
                  if (!q.AllowConversionErrors) throw new BMException (e, "{0}\r\nField={1}, type={2}.", e.Message, name, ft);
                  addConversionError(name, e);
               }
               sink.HandleValue(ctx, pfxField + name, value);
            }
            sink.HandleValue(ctx, pfxRecord, rdr);
            ctx.IncrementEmitted();
         }
      }

      /// <summary>
      /// Helper class to administrate Queries to be executed  
      /// </summary>
      public class Query
      {
         public readonly String Prefix;
         public readonly String Command;
         public readonly bool AllowConversionErrors;
         public readonly int? Timeout;
         public Query(XmlNode node, int? defaultTimeout)
         {
            Prefix = node.ReadStr("@prefix", null);
            Command = node.ReadStr("@cmd", null);
            if (Command == null)
               Command = node.ReadStr(null);
            AllowConversionErrors = node.ReadBool(1, "@allowconversionerrors", true);
            Timeout = SqlDatasource.ReadTimeoutFromNode(node, "@timeout", defaultTimeout);
         }
         public Query(String pfx, String cmd, int? timeout, bool allowConvErrors)
         {
            Prefix = pfx;
            Command = cmd;
            AllowConversionErrors = allowConvErrors;
            Timeout = timeout;
         }

         public DbCommand CreateCommand (DbConnection connection)
         {
            var cmd = connection.CreateCommand();
            cmd.CommandText = Command;
            if (Timeout != null)
               cmd.CommandTimeout = (int)Timeout;
            cmd.CommandType = CommandType.Text;
            return cmd;
         }
      }

      /// <summary>
      /// Helper class to administrate conversion exceptions in a field
      /// </summary>
      public class ConversionError
      {
         public readonly String Field;
         public readonly Exception Error;
         public int Count;

         public ConversionError(String field, Exception e)
         {
            Field = field;
            Error = e;
            Count = 1;
         }
      }
   }

   public class MysqlDatasource : SqlDatasource
   {
      protected override DbConnection createConnection()
      {
         return new MySqlConnection(getConnectionStringPlusTimeout());
      }
   }

   public class OdbcDatasource : SqlDatasource
   {
      protected override DbConnection createConnection()
      {
         var ret = new OdbcConnection(ConnectionString);
         if (ConnectionTimeout != null)
            ret.ConnectionTimeout = (int)ConnectionTimeout;
         return ret;
      }
   }
}
