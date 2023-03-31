# Simple test harness for Databricks SQL Execution API
This repo contains a simple harness for running a variable number of threads against a Databricks SQL Warehouse. The SQL statement used is embedded in the source file `RestDemoCmd.cs`.

### Preparing the Delta table
This test harness assumes you have a table called `field_demos.airlinedata.flights`. If you do not have such a Delta table, you can create it from the data that resides in `dbfs:/databricks-datasets/airlines`.

```
from pyspark.sql.functions import col, concat, lit, date_format

print("Loading database flights...")
initial_df = (spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/databricks-datasets/airlines/part-00000"))
df_schema = initial_df.schema
df = spark.read.schema(df_schema).csv("dbfs:/databricks-datasets/airlines/*")
newColumns = df.withColumn("DateString",concat(col("Year"),lit("-"),concat(col("Month"), lit("-"), col("DayofMonth")))).filter(col("Year").isNotNull())
airlineDataTemp = newColumns.withColumn("Date", date_format(col("DateString"), "yyyy-MM-dd")).drop(col("DateString"))
display(airlineDataTemp)


airlineDataTemp.write.partitionBy("Year", "UniqueCarrier").mode('overwrite').saveAsTable(f"{catalogName}.{dbName}.flights")

```
Note: create variables for catalogName and dbName that match the catalog and schema in which you wish to create the table.

## Running the test harness
The program has 5 command-line parameters:
1. The host of the warehouse/workspace
2. The warehouse_id (the last part of the "HTTP Path" in the [Connection Details tab](https://docs.databricks.com/sql/get-started/user-quickstart.html#get-sql-warehouse-connection-details))
3. [PAT token](https://docs.databricks.com/administration-guide/access-control/tokens.html)
4. Number of threads that will run the query concurrently
5. LIMIT of record count for each query


### Example

```
dotnet run my-db-workspace123.cloud.databricks.com wid123123 dbapic123123123 2 5
```
where:
* `my-db-workspace123.cloud.databricks.com` is the host of the workspace URL
* `wid123123` is the workspace_id (the last part of the SQL Warehouse Path)
* `dbapic123123123` is a Personal Access Token (PAT)
* `2` is the number of query threads to run
* `5` is the number of records (via LIMIT) for each query

# References
* [Databricks SQL Execution API](https://docs.databricks.com/sql/api/statements.html)
* [Generating a Personal Access Token (PAT)](https://docs.databricks.com/administration-guide/access-control/tokens.html)

