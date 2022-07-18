# SparkFlightSql

### Goals
The goal of the project is to offer a **SparkThriftServer** alternative based on Arrow flight SQL protocol. 
SparkThriftServer has a number of limitations, mainly that it's a centralized server that needs to pass all query results to the client through a single Spark Driver process.
**SparkFlightSql** aims to offer a distributed alternative where multiple servers will share the workload.

### Architecture
#### Metadata Queries
Metadata Queries are assumed not to require distributed serving and are always served with a single-endpoint FlightInfo.

#### Data Queries
All the heavy lifting of conversion between a Spark DataFrame and Arrow Record Batches fortunately has already been done in Spark.
DataFrame has a method `toArrowBatchRdd` (not part of public interface, though) that is used for Pandas UDFs.

Data Queries are assumed to require query distribution. A Client can submit a query to any active FlightServer.
The query is submitted to Spark Cluster when getFlightInfo endpoint is called and all active FlightServers are notified about the query.

It would be too cumbersome to offer up Flight Endpoints to the client from Spark Executor processes as executors are fundamentally assumed to be ephemeral. 
Instead of collecting query results to the driver, DataFrame is converted to an RDD of ArrowRecordBatches and each partition calls doPut endpoint of the *closest* active FlightServer.
(Note: Right now a random FlightServer is chosen)
Therefore, the client receives FlightInfo containing tickets to all active FlightServers. 
Each FlightServer passes record batches received from executors to an appropriate client and completes the query once the FlightServer that started the query sends the notification that query has completed.

![Alt text](https://www.plantuml.com/plantuml/svg/ZP1DQiCm44RtSueFPvsMw00bSN-MtMHfj13su9h8aXdBfL1yzo9NXPB8nCjX7ZEFTno3aJ3rbgfdsx4BccWmzSMqZEBTDunJWMyYe7gpNViHpYoVB7Z4UJ1omOjqSTmTDCoOfWDshJ0xW82_izZldr0bG1CjozhwgK7n-iNr5BoCyHK0rBuVl6CNKFs-IKHyz73IwVuzL2seS4EHp9-wqifoAiVDD5-NAgF-lL3gNoYrsArcKZfV2UMcJkNsJkLwfxHVl9BMIirRQex-CntPDLDlVm00)

#### Inter-Server Communication

There is a pluggable `ClusterManager` interface that governs how FlightServers communicate with each other

##### InMemoryClusterManager
`InMemoryClusterManager` requires no external dependencies. Communication between servers is implemented with doAction calls, however it's impossible to change the number of FlightServers once started.

* spark.flight.manager - static
* spark.flight.manager.static.peers - "localhost:8080,localhost:8080;localhost:8081,localhost:8081"

##### ZookeeperClusterManager
`ZookeeperClusterManager` requires an external Zookeeper cluster. There's no need to configure server locations upfront, instead servers use Zookeeper service to discover and communicate with each other once started.

* spark.flight.manager - zookeeper
* spark.flight.manager.zookeeper.url - localhost:10000
* spark.flight.manager.zookeeper.membershipPath - "/spark-flight-sql"

#### Usage
A single node server can be started by running `com.tokoko.spark.flight.SparkFlightSqlServer`.

Alternatively, there is a Docker Compose file provided in `dev` folder that starts 2-node Spark Standalone Cluster with separate Hive Metastore.
after running `docker compose up`, flight servers need to be started by `docker exec -ti dev-spark-worker-b-1 bash /opt/spark-apps/run.sh`
and `docker exec -ti dev-spark-worker-a-1 bash /opt/spark-apps/run.sh`

### Features
| Feature                                      | Status      |
|----------------------------------------------|-------------|
| Metadada Operations                          | In Progress |
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