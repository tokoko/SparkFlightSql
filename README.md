# Scope
The repo started out as **ArrowFlightSql** Server implementation with Apache Spark backend, but since then has evolved to encompass several independent components:
* **SparkFlightManager** - a lower-level utility that enables easier development of any kind of distributed Arrow Flight servers on top of Apache Spark
* **SparkFlightSql** - FlightSql implementation that builds on top of `SparkFlightManager`
* **FlightSql DataSourceV2** - Spark DataSourceV2 for FlightSql servers

# SparkFlightManager

`SparkFlightManager` is a pluggable interface that governs how individual FlightServers communicate with each other. The goal is to abstract away distributed execution of Spark DataFrames from the business logic expressed in Flight Servers.

All the heavy lifting of conversion between a Spark DataFrame and Arrow Record Batches fortunately has already been done in Spark.
DataFrame has a method `toArrowBatchRdd` (not part of public interface, though) that is used for Pandas UDFs.

`ZookeeperSparkFlightManager` is the only current implementation of `SparkFlightManager`. It requires an external Apache Zookeeper cluster. 
Flight servers use Zookeeper service to discover and communicate with each other once started. 
Metadata about running queries is also synchronized using Zookeeper.

For data exchange between individual Flight servers `FlightManager` uses an additional internal FlightServer that accompanies each public-facing one.
When FlightManager is instructed from one of the FlightServers to distribute a Spark DataFrame to the available cluster, it starts Spark action from the requesting node
but instead of collecting the results on the same node, DataFrame is instead converted to an RDD of ArrowRecordBatches and each partition calls doPut endpoint of the *closest* active internal FlightServer. (Note: Right now a random FlightServer is chosen)
Therefore, the client receives FlightInfo containing tickets to all active FlightServers.
Each FlightServer passes record batches that it's accompanying internal FlightServer received from executors to an appropriate client and the query finally completes once the FlightServer that started the query sends a notification that Spark action has completed.

* spark.flight.manager - zookeeper
* spark.flight.manager.zookeeper.url - localhost:10000
* spark.flight.manager.zookeeper.membershipPath - "/spark-flight-sql"

![Alt text](https://www.plantuml.com/plantuml/svg/ZP1DQiCm44RtSueFPvsMw00bSN-MtMHfj13su9h8aXdBfL1yzo9NXPB8nCjX7ZEFTno3aJ3rbgfdsx4BccWmzSMqZEBTDunJWMyYe7gpNViHpYoVB7Z4UJ1omOjqSTmTDCoOfWDshJ0xW82_izZldr0bG1CjozhwgK7n-iNr5BoCyHK0rBuVl6CNKFs-IKHyz73IwVuzL2seS4EHp9-wqifoAiVDD5-NAgF-lL3gNoYrsArcKZfV2UMcJkNsJkLwfxHVl9BMIirRQex-CntPDLDlVm00)

There's a reference FlightServer implementation in `com.tokoko.spark.flight.example.SparkParquetFlightProducer` that illustrates how a simple parquet reader server can be implemented using `SparkFlightManager`.

# SparkFlightSql

### Goals
The goal of the project is to offer a **SparkThriftServer** alternative based on Arrow flight SQL protocol. 
SparkThriftServer has a number of limitations, mainly that it's a centralized server that needs to pass all query results to the client through a single Spark Driver process.
**SparkFlightSql** aims to offer a distributed alternative where multiple servers will share the workload.

#### Metadata Queries
Metadata Queries are assumed not to require distributed serving and are always served with a single-endpoint FlightInfo.

#### Data Queries
Data Queries are assumed to require query distribution and therefore rely on `SparkFlightManager`. A client can submit a query to any active FlightServer.
The server that receives the query will create a DataFrame and invoke `SparkFlightManager` to distribute query results. (see above)

#### Usage
A single node server can be started by running `com.tokoko.spark.flight.SparkFlightSqlServer`.

Alternatively, there is a Docker Compose file provided in `dev` folder that starts 2-node Spark Standalone Cluster with separate Hive Metastore.
after running `docker compose up`, flight servers need to be started by `docker exec -ti dev-spark-worker-b-1 bash /opt/spark-apps/run.sh`
and `docker exec -ti dev-spark-worker-a-1 bash /opt/spark-apps/run.sh`

### Features
| Feature                                      | Status      |
|----------------------------------------------|-------------|
| Metadata Operations                          | In Progress |
| Read-Only Operations                         | In Progress |
| Distributed Server                           | In Progress |
| Execution Mode configurable per query (auto) | Planned     |
| Prepared Statements                          | Planned     |
| Streaming Queries                            | Planned     |
| Spark UI Plugin                              | Planned     |
| ZooKeeper Integration                        | In Progress |
| DML Operations                               | Planned     |
| Basic Authentication                         | In Progress |
| LDAP Authentication                          | Planned     |
| Kerberos Authentication                      | Planned     |
| Ranger Integration                           | Planned     |
| Storage Impersonation                        | Planned     |
| Spark DataSourceV2                           | In Progress |