using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.SqlServer.Management.Smo;
using System.Data.SqlClient;

namespace CustomFusionLoader
{
    class Program
    {
        static void foo()
        {
            Database db = CommonUtils.get_database("PC", "fusion");
            string path = Path.GetFullPath(@"C:\Users\user\data\innocentive\fusion\training_sample\8567\Spectrum\PhoConSpecAbsmag_1.csv");

            SignalReader ldr = new SignalReader(db, path);
            SqlConnection sql_conn = CommonUtils.get_sql_connection("PC", "fusion");
            sql_conn.Open();
            SqlBulkCopy bulkCopy = new SqlBulkCopy(sql_conn);
            bulkCopy.DestinationTableName = "[fusion].[Fact].[Signal]";
            bulkCopy.BatchSize = 10000;

            bulkCopy.WriteToServer(ldr);

            bulkCopy.Close();


        }
        static void goo()
        {
            Database db = CommonUtils.get_database("PC", "fusion");

            string dir_path = Path.GetFullPath(@"C:\Users\user\data\innocentive\fusion\training_sample\8567\Spectrum");
            DirectoryInfo dir = new DirectoryInfo(dir_path);
            foreach (FileInfo item in dir.EnumerateFiles())
            {
                string path = item.FullName;
                SignalReader ldr = new SignalReader(db, path);
                SqlConnection sql_conn = CommonUtils.get_sql_connection("PC", "fusion");
                sql_conn.Open();
                SqlBulkCopy bulkCopy = new SqlBulkCopy(sql_conn);
                bulkCopy.DestinationTableName = "[fusion].[Fact].[Signal]";
                bulkCopy.BatchSize = 10000;

                bulkCopy.WriteToServer(ldr);
                bulkCopy.Close();
            }
        }
        static void joo()
        {
            

            string root_path = Path.GetFullPath(@"C:\Users\user\data\innocentive\fusion\training_sample");
            DirectoryInfo root = new DirectoryInfo(root_path);
            foreach (DirectoryInfo shotdir in root.EnumerateDirectories())
            {
                Database db = CommonUtils.get_database("PC", "fusion");
                Console.WriteLine(shotdir.Name);
                DirectoryInfo spec_dir = new DirectoryInfo(Path.Combine(shotdir.FullName, "Spectrum"));
                foreach (FileInfo signal_file in spec_dir.EnumerateFiles())
                {
                    string path = signal_file.FullName;
                    SignalReader ldr = new SignalReader(db, path);
                    SqlConnection sql_conn = CommonUtils.get_sql_connection("PC", "fusion");
                    sql_conn.Open();
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(sql_conn);
                    bulkCopy.DestinationTableName = "[fusion].[Fact].[Signal]";
                    bulkCopy.BatchSize = 10000;

                    bulkCopy.WriteToServer(ldr);
                    bulkCopy.Close();
                }
                DirectoryInfo ts_dir = new DirectoryInfo(Path.Combine(shotdir.FullName, "TimeSeries"));
                foreach (FileInfo signal_file in ts_dir.EnumerateFiles())
                {
                    string path = signal_file.FullName;
                    SignalReader ldr = new SignalReader(db, path);
                    SqlConnection sql_conn = CommonUtils.get_sql_connection("PC", "fusion");
                    sql_conn.Open();
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(sql_conn);
                    bulkCopy.DestinationTableName = "[fusion].[Fact].[Signal]";
                    bulkCopy.BatchSize = 10000;

                    bulkCopy.WriteToServer(ldr);
                    bulkCopy.Close();
                }
            }
                    }
        static void Main(string[] args)
        {
            CommonUtils.cwd();
            joo();
            CommonUtils.user_exit();

        }
    }
}
