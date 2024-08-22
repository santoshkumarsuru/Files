using System;
using System.Data;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;

namespace OracleToSqlServer
{
    class Program
    {
        static void Main(string[] args)
        {
            string oracleConnectionString = "User Id=your_user;Password=your_password;Data Source=your_oracle_data_source;";
            string sqlServerConnectionString = "Server=your_sql_server;Database=your_database;User Id=your_user;Password=your_password;";

            using (OracleConnection oracleConnection = new OracleConnection(oracleConnectionString))
            {
                oracleConnection.Open();

                using (SqlConnection sqlServerConnection = new SqlConnection(sqlServerConnectionString))
                {
                    sqlServerConnection.Open();

                    // Specify the table you want to transfer
                    string tableName = "YOUR_TABLE_NAME";
                    ExportTable(oracleConnection, sqlServerConnection, tableName);
                }
            }
        }

        static void ExportTable(OracleConnection oracleConnection, SqlConnection sqlServerConnection, string tableName)
        {
            // Step 1: Fetch data from Oracle
            string oracleQuery = $"SELECT * FROM {tableName}";
            using (OracleCommand oracleCommand = new OracleCommand(oracleQuery, oracleConnection))
            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
            {
                // Step 2: Create the table in SQL Server
                CreateTableInSqlServer(sqlServerConnection, oracleReader, tableName);

                // Step 3: Insert data into SQL Server
                InsertDataIntoSqlServer(sqlServerConnection, oracleReader, tableName);
            }
        }

        static void CreateTableInSqlServer(SqlConnection sqlServerConnection, OracleDataReader oracleReader, string tableName)
        {
            // Create a command to create the table in SQL Server
            string createTableQuery = $"CREATE TABLE {tableName} (";

            for (int i = 0; i < oracleReader.FieldCount; i++)
            {
                string columnName = oracleReader.GetName(i);
                string dataType = MapOracleTypeToSqlServer(oracleReader.GetFieldType(i));

                createTableQuery += $"{columnName} {dataType},";
            }

            // Remove the last comma and close the statement
            createTableQuery = createTableQuery.TrimEnd(',') + ")";

            using (SqlCommand sqlCommand = new SqlCommand(createTableQuery, sqlServerConnection))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }

        static string MapOracleTypeToSqlServer(Type oracleType)
        {
            // Map Oracle data types to SQL Server data types
            if (oracleType == typeof(string)) return "NVARCHAR(MAX)";
            if (oracleType == typeof(int)) return "INT";
            if (oracleType == typeof(DateTime)) return "DATETIME";
            if (oracleType == typeof(decimal)) return "DECIMAL";
            if (oracleType == typeof(double)) return "FLOAT";
            // Add more mappings as needed
            return "NVARCHAR(MAX)"; // Default mapping
        }

        static void InsertDataIntoSqlServer(SqlConnection sqlServerConnection, OracleDataReader oracleReader, string tableName)
        {
            while (oracleReader.Read())
            {
                string insertQuery = $"INSERT INTO {tableName} VALUES (";

                for (int i = 0; i < oracleReader.FieldCount; i++)
                {
                    object value = oracleReader.GetValue(i);
                    insertQuery += value is string ? $"'{value}'" : value.ToString();
                    insertQuery += ",";
                }

                insertQuery = insertQuery.TrimEnd(',') + ")";

                using (SqlCommand sqlCommand = new SqlCommand(insertQuery, sqlServerConnection))
                {
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}
