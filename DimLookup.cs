using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomFusionLoader
{
    class DimLookup
    {
        public static int DimLookupx(Database db, string dim_table_name, string dim_id_name, Dictionary<string, object> dim_values)
        {
            string sel_stmt = string.Format("SELECT {0} FROM Dim.{1}", dim_id_name, dim_table_name);
            string where_clause = "";
            string values_clause = "";
            string columns_clause = "";
            if (dim_values.Count > 0)
            {
                foreach (var item in dim_values)
                {
                    if (where_clause.Length == 0)
                    {
                        where_clause = string.Format("\nWHERE {0} = {1}", item.Key, item.Value);
                        values_clause = string.Format("{0}", item.Value);
                        columns_clause = string.Format("{0}", item.Key);
                    }
                    else
                    {
                        where_clause += string.Format("\nAND {0} = {1}", item.Key, item.Value);
                        values_clause += string.Format(", {0}", item.Value);
                        columns_clause += string.Format(", {0}", item.Key);
                    }
                }
            }

            sel_stmt += where_clause;
            int id;
            Table dim_table = db.Tables[dim_table_name, "Dim"];
            SqlConnection sql_conn = CommonUtils.get_sql_connection("PC", "fusion");
            sql_conn.Open();
            SqlCommand command = sql_conn.CreateCommand();
            command.CommandText = sel_stmt;
            command.CommandTimeout = 15;
            command.CommandType = CommandType.Text;
            object resultx = command.ExecuteScalar();

            if (resultx != null)
            {
                id = (int)resultx;
            }
            else
            {
                SqlCommand ins_comm = sql_conn.CreateCommand();
                ins_comm.CommandText = string.Format("INSERT INTO Dim.{0} ({1}) VALUES ({2}); SELECT CAST(scope_identity() AS int)", dim_table_name, columns_clause, values_clause);
                Console.WriteLine("INSERT: {0}", ins_comm.CommandText);
                ins_comm.CommandTimeout = 15;
                ins_comm.CommandType = CommandType.Text;
                id = (int)ins_comm.ExecuteScalar();
            }
            return id;
        }
        public static int DimLookupEntry(Database db, string dim_table_name, string dim_id_name, string dim_value)
        {
            int id;
            Table dim_table = db.Tables[dim_table_name, "Dim"];
            SqlConnection sql_conn = CommonUtils.get_sql_connection("PC", "fusion");
            sql_conn.Open();
            SqlCommand command = sql_conn.CreateCommand();
            command.CommandText = string.Format("SELECT {0} FROM Dim.{1} WHERE name = {2};", dim_id_name, dim_table_name, dim_value);
            command.CommandTimeout = 15;
            command.CommandType = CommandType.Text;
            object resultx = command.ExecuteScalar();
            
            if (resultx != null)
            {
                object[] obj = new object[10] ;
                Console.WriteLine("search for Dim ID {0}", resultx.ToString());
                id = (int)resultx; 
            }
            else
            {
                //rdr.Close();
                SqlCommand ins_comm = sql_conn.CreateCommand();
                ins_comm.CommandText = string.Format("INSERT INTO Dim.{0} VALUES({1}); SELECT CAST(scope_identity() AS int)", dim_table_name, dim_value);
                ins_comm.CommandTimeout = 15;
                ins_comm.CommandType = CommandType.Text;
                id = (int)ins_comm.ExecuteScalar();
            }
            sql_conn.Close();
            return id;
        }
    }
}
