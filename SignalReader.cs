using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using Microsoft.SqlServer.Management.Smo;//Microsoft.SqlServer.Management.Smo

namespace CustomFusionLoader
{
    public class SignalReader : IDataReader
    {
        StreamReader SignalStreamReader { get; set; }
        string CurrentLine { get; set; }
        int SignalInfoID { get; set; }
        int ShotNo { get; set; }
        string XType { get; set; }
        Database DestinationDatabase { get; set; }
        #region IDataReader helpers
        private Dictionary<string, int> fact_column { get; set; }
        private Dictionary<string, Type> fact_type { get; set; }
        public bool Eof { get; private set; }
        protected object[] Values { get; set; }
        #endregion
        #region SignalReader
        public SignalReader(Database db, string path)
        {
            Console.WriteLine("SignalLoader({0},{1})", db.Name, path);
            DestinationDatabase = db;
            FilePath = path;
            SignalStreamReader = new StreamReader(path);
            ShotNo = CommonUtils.get_shot_no(path);
            XType = "'" + CommonUtils.get_x_type(path) + "'";
            CurrentLine = SignalStreamReader.ReadLine();
            ProcessHeader();
        }
        void ProcessHeader()
        {
            Console.WriteLine("\tProcessHeader");
            Dictionary<string, string> dim_entry = new Dictionary<string, string>();
            string[] dim_item = new string[2];
            while (!CurrentLine.Contains("Value"))
            {
                Console.WriteLine("\t\t{0}", CurrentLine);
                dim_item = CurrentLine.Split(':');
                dim_entry.Add(dim_item[0].Trim(), dim_item[1].Trim().Replace("\"","'"));
                CurrentLine = SignalStreamReader.ReadLine();
            }
            int XUnitsID = DimLookup.DimLookupEntry(DestinationDatabase, "Units", "UnitsID", dim_entry["XUnits"]);
            int UnitsID = DimLookup.DimLookupEntry(DestinationDatabase, "Units", "UnitsID", dim_entry["Units"]);
            int XTypeID = DimLookup.DimLookupEntry(DestinationDatabase, "XType", "XTypeID", XType);
            Dictionary<string, object> col_vals = new Dictionary<string, object>();
            col_vals.Add("Name", dim_entry["Name"]);
            col_vals.Add("Delta", dim_entry["Delta"]);
            col_vals.Add("[Left]", dim_entry["Left"]);
            col_vals.Add("XUnitsID",XUnitsID);
            col_vals.Add("UnitsID", UnitsID);
            col_vals.Add("XTypeID", XTypeID);
            SignalInfoID = DimLookup.DimLookupx(DestinationDatabase, "SignalInfo", "SignalInfoID", col_vals);
            fact_column = new Dictionary<string, int>();
            fact_column.Add("SignalInfoID", 0);
            fact_column.Add("ShotNo", 1);
            fact_column.Add("i", 2);
            fact_column.Add("Yi", 3);
            fact_type = new Dictionary<string, Type>();
            fact_type.Add("SignalInfoID", Type.GetType("System.Int32"));
            fact_type.Add("ShotNo", Type.GetType("System.Int32"));
            fact_type.Add("i", Type.GetType("System.Int32"));
            fact_type.Add("Yi", Type.GetType("System.Double"));
            Values = new object[4];
            Values[fact_column["SignalInfoID"]] = SignalInfoID;
            Values[fact_column["ShotNo"]] = ShotNo;
            Values[fact_column["i"]] = 0;
            Values[fact_column["Yi"]] = 0;
            Console.WriteLine("\tProcessHeader complete");
        }
        #endregion
        #region IDataReader Properties
        public int Depth
        {
            get
            {
                return 0;
            }
        }

        public bool IsClosed
        {
            get
            {
                return DestinationDatabase.IsAccessible;
            }
        }

        public int RecordsAffected
        {
            get;
            private set;
        }

        public int FieldCount
        {
            get
            {
                return 4;
            }
        }

        public int LineCount { get; private set; }
        public string FilePath { get; private set; }

        public object this[string name]
        {
            get
            {
                return Values[fact_column[name]];
            }
        }

        public object this[int i]
        {
            get
            {
                return Values[i];
            }
        }
        #endregion
        #region IDataReader Methods

        public void Close()
        {
            throw new NotImplementedException();
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            CurrentLine = SignalStreamReader.ReadLine();
            Values[fact_column["i"]] = LineCount;
            Eof = CurrentLine == null;
            double x = double.NaN;
            if (!Eof)
            {
                Values[fact_column["Yi"]] = double.TryParse(CurrentLine.Trim(), out x);
                LineCount += 1;
            }
            return !Eof;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            return Values[i];
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
