using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace RestDemo
{
    /**
     * Databricks SQL Execution API POST parameters used for submitting a new query
     */
    class SqlRequest
    {
        public string statement { get; set; }
        public string warehouse_id { get; set; }
        public string catalog { get; set; }
        public string schema { get; set; }
        public string wait_timeout = "0s";
    }

    /**
     * The return payload submitting a new query
     */
    class SqlResponse
    {
        public string statement_id { get; set; }
    }

    /**
     * Flight class used when converting the JSON result payload to an object
     */
    class Flight
    {
        public Flight(JToken x)
        {
            Year = (string)x[0];
            Month = (string)x[1];
            DayofMonth = (string)x[2];
            DepTime = (string)x[3];
            ArrTime = (string)x[4];
            FlightNum = (string)x[5];
            AirTime = (string)x[6];
            ArrDelay = (string)x[7];
            DepDelay = (string)x[8];
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

    class RestDemo
    {

        static void Main(string[] args)
        {
            string host = args[0];
            string path = args[1];
            string token = args[2];
            int loops = int.Parse(args.Length < 4 ? "1" : args[3]);
            string limit = args.Length < 5 ? "1" : args[4];

            List<Task> threads = new List<Task>();
            for (int i = 0; i < loops; i++)
            {
                Console.WriteLine("starting thread");
                threads.Add(RunAsync(host, path, token, limit));
            }
            Parallel.ForEach(threads, t => t.GetAwaiter().GetResult());
        }

        static async Task RunAsync(string host, string path, string token, string limit)
        {
            HttpClient client = new HttpClient();

            client.BaseAddress = new Uri($"https://{host}");
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            string sql = $"select Year, Month, DayofMonth, DepTime, ArrTime, FlightNum, AirTime, ArrDelay, DepDelay from field_demos.airlinedata.flights limit {limit}";

            // Submit query to warehouse (note: results are retrieved using another endpoint)
            SqlRequest s = new SqlRequest();
            s.statement = sql;
            s.warehouse_id = path;
            s.catalog = "field_demos";
            s.schema = "airlinedata";
            DateTime t_start = DateTime.Now;
            // Retrieve query execution response
            SqlResponse r = await GetSqlResponse(client, s);

            bool is_running = true;

            string chunk_url = $"/api/2.0/sql/statements/{r.statement_id}";
            while (is_running)
            {
                HttpResponseMessage response = await client.GetAsync(chunk_url);
                response.EnsureSuccessStatusCode();
                string response_str = await response.Content.ReadAsStringAsync();
                JObject jsonResponse = JsonConvert.DeserializeObject<JObject>(response_str);
                string s_state = (string)jsonResponse["status"]["state"];
                Console.WriteLine($"Query state: {s_state}");
                is_running = s_state == null || s_state.Equals("PENDING") || s_state.Equals("RUNNING");
                is_running = is_running && !s_state.Equals("FAILED");
                if (!is_running)
                {
                    DateTime t_end = DateTime.Now;
                    List<Flight> flights = new List<Flight>();
                    foreach (var el in jsonResponse["result"]["data_array"])
                    {
                        Flight f = new Flight(el);
                        flights.Add(f);
                    }
                    Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} flights: \n\t{String.Join("\n\t", flights)}");
                    Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} Time Taken: {(t_end - t_start)}");

                    // keep looping for next chunk
                    if (jsonResponse["next_chunk_internal_link"] != null)
                    {
                        chunk_url = (string)jsonResponse["next_chunk_internal_link"];
                        is_running = true;
                        Console.WriteLine($"next_chunk_internal_link={chunk_url}");
                    }
                }
            }

        }

        static async Task<SqlResponse> GetSqlResponse(HttpClient client, SqlRequest statement)
        {
            HttpResponseMessage response = await client.PostAsJsonAsync("/api/2.0/sql/statements/", statement);
            response.EnsureSuccessStatusCode();
            SqlResponse r = await response.Content.ReadAsAsync<SqlResponse>();
            return r;
        }
    }
}
