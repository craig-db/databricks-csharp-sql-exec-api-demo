using System;
using System.Data.Odbc;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;


namespace ODBCDemo
{

    /**
     * Flight class used when converting the JSON result payload to an object
     */
    class Flight
    {
        public Flight(OdbcDataReader x)
        {
            Year = x.GetString(0);
            Month = x.GetString(1);
            DayofMonth = x.GetString(2);
            DepTime = x.GetString(3);
            ArrTime = x.GetString(4);
            FlightNum = x.GetString(5);
            AirTime = x.GetString(6);
            ArrDelay = x.GetString(7);
            DepDelay = x.GetString(8);
        }
        public string Year { get; set; }
        public string Month { get; set; }
        public string DayofMonth { get; set; }
        public string DepTime { get; set; }
        public string ArrTime { get; set; }
        public string FlightNum { get; set; }
        public string AirTime { get; set; }
        public string ArrDelay { get; set; }
        public string DepDelay { get; set; }

        override public string ToString()
        {
            return $"""FlightNum: {FlightNum}, ArrTime: {ArrTime}, DepTime: {DepTime}, AirTime: {AirTime}, ArrDelay: {ArrDelay}, DepDelay: {DepDelay}, Year: {Year}, Month: {Month}, DayofMonth: {DayofMonth}""";
        }
    }

    class ODBCDemo
    {
        static void Main(string[] args)
        {
            string dsn = args[0];
            int loops = int.Parse(args.Length < 2 ? "1" : args[1]);
            string limit = args.Length < 3 ? "1" : args[2];
            string queryString = $"select Year, Month, DayofMonth, DepTime, ArrTime, FlightNum, AirTime, ArrDelay, DepDelay from airlinedata.flights limit {limit}";
            List<Task> threads = new List<Task>();
            for (int i = 0; i < loops; i++)
            {
                Console.WriteLine("starting thread");
                Console.Out.Flush();
                threads.Add(RunAsync(dsn, queryString));
            }
            Parallel.ForEach(threads, t => t.GetAwaiter().GetResult());
        }

        static async Task RunAsync(string dsn, string queryString)
        {
            OdbcConnection DbConnection = new OdbcConnection($"DSN={dsn}");
            await DbConnection.OpenAsync();
            OdbcCommand DbCommand = DbConnection.CreateCommand();
            DbCommand.CommandText = queryString;
            DateTime t_start = DateTime.Now;
            OdbcDataReader DbReader = DbCommand.ExecuteReader();
            int fCount = DbReader.FieldCount;
            DateTime t_end = DateTime.Now;
            List<Flight> flights = new List<Flight>();

            while (DbReader.Read())
            {
                Flight f = new Flight(DbReader);
                flights.Add(f);
            } 
            Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} flights: \n\t{String.Join("\n\t", flights)}");
            Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} Time Taken: {(t_end - t_start)}");

        }
    }

}
