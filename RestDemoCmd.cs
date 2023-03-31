using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace RestDemo
{
    class SqlRequest
    {
        public string statement { get; set; }
        public string warehouse_id { get; set; }
        public string catalog { get; set; }
        public string schema { get; set; }
        public string wait_timeout = "0s";
    }

    class Manifest
    {
        public int total_chunk_count { get; set; }
    }

    class SqlResponse
    {
        public string statement_id { get; set; }
        public Manifest manifest { get; set; }
    }

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
        static string host = "";
        static string path = "";
        static string token = "";
        static string limit = "";

        static void Main(string[] args)
        {
            host = args[0];
            path = args[1];
            token = args[2];
            int loops = int.Parse(args.Length < 4 ? "1" : args[3]);
            limit = args.Length < 5 ? "1" : args[4];
            List<Task> threads = new List<Task>();
            for (int i = 0; i < loops; i++)
            {
               Console.WriteLine("starting thread");
               threads.Add(RunAsync());
            }
            Parallel.ForEach(threads, t => t.GetAwaiter().GetResult());
        }

        static async Task RunAsync()
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

            while (is_running)
            {
                HttpResponseMessage response = await client.GetAsync($"/api/2.0/sql/statements/{r.statement_id}");
                response.EnsureSuccessStatusCode();
                string response_str = await response.Content.ReadAsStringAsync();
                JObject response_jo = JObject.Parse(response_str);
                JObject jsonResponse = JsonConvert.DeserializeObject<JObject>(response_str);
                string s_state = response_jo.SelectToken("$.status.state").Value<string>();

                is_running = s_state == null || s_state.Equals("PENDING") || s_state.Equals("RUNNING");
                is_running = is_running && !s_state.Equals("FAILED");
                if (!is_running)
                {
                    List<Flight> flights = new List<Flight>();
                    foreach (var el in jsonResponse["result"]["data_array"])
                    {
                        Flight f = new Flight(el);
                        flights.Add(f);
                    }
                    Console.WriteLine($"flights: {String.Join("\n", flights)}");
                }
            }
            DateTime t_end = DateTime.Now;
            Console.WriteLine($"Time Taken: {(t_end - t_start)}");

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
