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
      public String ConnectionString;
      public List<Query> Queries;
      public void Init(PipelineContext ctx, XmlNode node)
      {
         ConnectionString = node.ReadStr("connection");
         XmlNodeList ch = node.SelectNodes("query");
         if (ch.Count > 0)
         {
            Queries = new List<Query>(ch.Count);
            foreach (XmlNode n in ch) Queries.Add(new Query(n));
         }
      }

      protected virtual DbConnection createConnection ()
      {
         return new SqlConnection(ConnectionString);
      }

      public void Import(PipelineContext ctx, IDatasourceSink sink)
      {
         DbConnection connection = null;
         try
         {
            connection = createConnection();
            connection.Open();

            if (Queries == null)
               EmitTables(ctx, connection);
            else
               foreach (Query q in Queries)
                  EmitQuery(ctx, connection, q.Command, q.Prefix);
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
         EmitQuery(ctx, connection, String.Format("select * from {0};", table), table);
      }

      protected void EmitQuery(PipelineContext ctx, DbConnection connection, String q, String prefix)
      {
         var cmd = connection.CreateCommand();
         cmd.CommandText = q;
         cmd.CommandTimeout = 15;
         cmd.CommandType = CommandType.Text;

         ctx.SendItemStart (cmd);
         using (DbDataReader rdr = executeReader(ctx, cmd))
         {
            EmitRecords(ctx, rdr, prefix);
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
            throw;
         }
      }

      protected void EmitRecords(PipelineContext ctx, DbDataReader rdr, String prefix)
      {
         String pfxRecord = String.IsNullOrEmpty(prefix) ? "record" : prefix;
         String pfxField = pfxRecord + "/";
         var sink = ctx.Pipeline;
         while (rdr.Read())
         {
            int fcnt = rdr.FieldCount;
            for (int i = 0; i < fcnt; i++)
            {
               String name = rdr.GetName(i);
               Type ft = rdr.GetFieldType(i);
               Object value = rdr.GetValue(i);
               sink.HandleValue(ctx, pfxField + name, value);
            }
            sink.HandleValue(ctx, pfxRecord, rdr);
            ctx.IncrementEmitted();
         }
      }

      public class Query
      {
         public readonly String Prefix;
         public readonly String Command;
         public Query (XmlNode node)
         {
            Prefix = node.ReadStr("@prefix", null);
            Command = node.ReadStr("@cmd", null);
            if (Command==null) 
               Command = node.ReadStr(null);
         }
      }
   }

   public class MysqlDatasource : SqlDatasource
   {
      protected override DbConnection createConnection()
      {
         return new MySqlConnection(ConnectionString);
      }
   }
   public class OdbcDatasource : SqlDatasource
   {
      protected override DbConnection createConnection()
      {
         return new OdbcConnection(ConnectionString);
      }
   }
}
