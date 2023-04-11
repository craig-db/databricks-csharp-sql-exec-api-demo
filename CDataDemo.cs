using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient; 
using System.Data.CData.Databricks;

namespace DapperDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            DatabricksConnectionStringBuilder builder =
                new DatabricksConnectionStringBuilder("Server=127.0.0.1;HTTPPath=MyHTTPPath;User=MyUser;Token=MyToken;");
                //Pass the connection string builder an existing connection string, and you can get and set any of the elements as strongly typed properties.
            
            string connectionString =  "User=alexander.gauthier@databricks.com;"+
                                        "Password=myPassword;"+
                                        "Server=https://adb-8583543493330154.14.azuredatabricks.net;"+
                                        "HTTPPath=/sql/1.0/warehouses/c5e0b2c7b942bc59"+
                                        "Database=crimes;"+
                                        "Url=https://adb-8583543493330154.14.azuredatabricks.net;"+
                                        "AuthScheme=token;"+
                                        "Token=dapi1d819985de50aab3057a0330c740c73f";

            string queryString = "SELECT * from crimes.chicago";

            
        using (DbConnection connection = new DatabricksConnection(connectionString))
        {
           DbCommand command = connection.CreateCommand();
            command.CommandText = queryString;
            connection.Open();

            using (DbDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    // Process the data...
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Console.WriteLine("{0} = {1}", reader.GetName(i), reader.GetValue(i));
                    }
                }

                reader.CloseAsync();
            }
        }
        }
    }
}
