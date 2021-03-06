package com.tokoko.spark.flight.sql

import com.tokoko.spark.flight.utils.TestUtils
import org.apache.arrow.flight.sql.{FlightSqlClient, FlightSqlProducer}
import org.apache.arrow.flight.sql.util.TableRef
import org.apache.arrow.flight.{CallStatus, FlightClient, FlightInfo, FlightProducer, FlightServer}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector}
import org.apache.curator.test.TestingServer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable

class SparkFlightSqlProducerMetadataSuite extends AnyFunSuite with BeforeAndAfterAll {

  var clients: Seq[FlightClient] = _
  var servers: Seq[FlightServer] = _
  var producers: Seq[FlightProducer] = _
  var rootAllocator: BufferAllocator = _
  var spark: SparkSession = _
  var zkServer: TestingServer = _
  var headClient: FlightSqlClient = _

  override def beforeAll(): Unit = {
    val zookeeperPort = 9822
    zkServer = new TestingServer(zookeeperPort, true)

    zkServer.restart()

    val spark = SparkSession.builder
      .master("local")
      .config("spark.sql.catalog.test_catalog", "com.tokoko.spark.flight.utils.TestCatalog")
      .config("spark.sql.catalog.test_catalog.test_db", "test_table1,test_table2")
      .config("spark.sql.catalog.test_catalog.test_db2", "test_table3,test_table4")
      .appName("SparkFlightSqlServer").getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .delete(new Path("local/testtable"), true)

    spark.range(10).toDF("id").write
      .option("path", "local/testtable")
      .mode("overwrite")
      .saveAsTable("testtable")

    rootAllocator = new RootAllocator(Long.MaxValue)

    val setup = TestUtils.startServersCommon(rootAllocator, spark, Seq(9914, 9916), "basic", "zookeeper", zookeeperPort.toString, "sql")

    servers = setup._1
    clients = setup._2
    producers = setup._3

    headClient = new FlightSqlClient(clients.head)
  }

  // TODO tables - tableTypes
  // TODO table types

  test("getCatalogs returns spark_catalog and other configured V2 plugged catalogs") {
    val client = headClient
    val fi = client.getCatalogs()
    val stream = client.getStream(fi.getEndpoints.get(0).getTicket)

    val catalogs: mutable.Set[String] = mutable.Set.empty

    while (stream.next) {
      val root = stream.getRoot
      val nameVector: VarCharVector = root.getVector("catalog_name").asInstanceOf[VarCharVector]

      for (i <- 0 until root.getRowCount) {
        catalogs.add(new String(nameVector.get(i), StandardCharsets.UTF_8))
      }
    }

    assert(catalogs == mutable.Set("spark_catalog", "test_catalog"))
  }

  def getSchemasOutput(fi: FlightInfo): mutable.Set[(String, String)] = {
    val client = clients.head
    val stream = client.getStream(fi.getEndpoints.get(0).getTicket)

    val schemas: mutable.Set[(String, String)] = mutable.Set.empty

    while (stream.next) {
      val root = stream.getRoot
      val catalogVector: VarCharVector = root.getVector("catalog_name").asInstanceOf[VarCharVector]
      val schemaVector: VarCharVector = root.getVector("db_schema_name").asInstanceOf[VarCharVector]

      for (i <- 0 until root.getRowCount) {
        schemas.add(
          (
            new String(catalogVector.get(i), StandardCharsets.UTF_8),
            new String(schemaVector.get(i), StandardCharsets.UTF_8)
          )
        )
      }
    }
    schemas
  }

  test("getSchemas for all catalogs") {
    val client = headClient
    val fi = client.getSchemas("", null)
    val schemas = getSchemasOutput(fi)

    assert(schemas == mutable.Set(
      ("spark_catalog", "default"),
      ("test_catalog", "test_db"),
      ("test_catalog", "test_db2")
    ))

  }

  test("getSchemas for a single catalog") {
    val client = headClient
    val fi = client.getSchemas("test_catalog", null)
    val schemas = getSchemasOutput(fi)

    assert(schemas == mutable.Set(
      ("test_catalog", "test_db"),
      ("test_catalog", "test_db2")
    ))

  }

  test("getSchemas for all catalogs filtered") {
    val client = headClient
    val fi = client.getSchemas("", "test%")
    val schemas = getSchemasOutput(fi)

    assert(schemas == mutable.Set(
      ("test_catalog", "test_db"),
      ("test_catalog", "test_db2")
    ))

  }

  test("getTables returns all tables without schemas") {
    val client = headClient
    val fi = client.getTables("", null, null, null, false)
    val stream = client.getStream(fi.getEndpoints.get(0).getTicket)

    val tables: mutable.Set[(String, String, String, String)] = mutable.Set.empty

    while (stream.next) {
      val root = stream.getRoot
      val catalogNameVector: VarCharVector = root.getVector("catalog_name").asInstanceOf[VarCharVector]
      val schemaNameVector: VarCharVector = root.getVector("db_schema_name").asInstanceOf[VarCharVector]
      val tableNameVector: VarCharVector = root.getVector("table_name").asInstanceOf[VarCharVector]
      val tableTypeVector: VarCharVector = root.getVector("table_type").asInstanceOf[VarCharVector]

      for (i <- 0 until root.getRowCount) {
        tables.add(
          (
            new String(catalogNameVector.get(i), StandardCharsets.UTF_8),
            new String(schemaNameVector.get(i), StandardCharsets.UTF_8),
            new String(tableNameVector.get(i), StandardCharsets.UTF_8),
            new String(tableTypeVector.get(i), StandardCharsets.UTF_8)
          )
        )
      }
    }

    assert(tables == mutable.Set(
      ("spark_catalog", "default", "testtable", "MANAGED"),
      ("test_catalog", "test_db", "test_table1", "MANAGED"),
      ("test_catalog", "test_db", "test_table2", "MANAGED"),
      ("test_catalog", "test_db2", "test_table3", "MANAGED"),
      ("test_catalog", "test_db2", "test_table4", "MANAGED")
    ))
  }

  test("getTables returns table with schema") {
    val client = headClient
    val fi = client.getTables("spark_catalog", null, null, null, true)
    val stream = client.getStream(fi.getEndpoints.get(0).getTicket)

    val tables: mutable.Set[(String, String, String, String, Schema)] = mutable.Set.empty

    while (stream.next) {
      val root = stream.getRoot
      val catalogNameVector: VarCharVector = root.getVector("catalog_name").asInstanceOf[VarCharVector]
      val schemaNameVector: VarCharVector = root.getVector("db_schema_name").asInstanceOf[VarCharVector]
      val tableNameVector: VarCharVector = root.getVector("table_name").asInstanceOf[VarCharVector]
      val tableTypeVector: VarCharVector = root.getVector("table_type").asInstanceOf[VarCharVector]
      val tableSchemaVector: VarBinaryVector = root.getVector("table_schema").asInstanceOf[VarBinaryVector]

      for (i <- 0 until root.getRowCount) {
        val schema = MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(
            tableSchemaVector.getObject(i)
          ))))

        tables.add(
          (
            new String(catalogNameVector.get(i), StandardCharsets.UTF_8),
            new String(schemaNameVector.get(i), StandardCharsets.UTF_8),
            new String(tableNameVector.get(i), StandardCharsets.UTF_8),
            new String(tableTypeVector.get(i), StandardCharsets.UTF_8),
            schema
          )
        )
      }
    }

    val expectedSchema = new Schema(
      List(
        Field.nullable("id", MinorType.BIGINT.getType)
      ).asJava
    )

    assert(tables == mutable.Set(
      ("spark_catalog", "default", "testtable", "MANAGED", expectedSchema)
    ))
  }

  def assertEmpty(flightInfo: FlightInfo): Unit = {
    val client = clients.head
    val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket)

    while (stream.next) {
      assert(stream.getRoot.getRowCount == 0)
    }
  }

  test("key requests are always empty") {
    val client = headClient
    val tableRef = TableRef.of("spark_catalog", "default", "testtable")
    assertEmpty(client.getPrimaryKeys(tableRef))
    assertEmpty(client.getImportedKeys(tableRef))
    assertEmpty(client.getExportedKeys(tableRef))
    assertEmpty(client.getCrossReference(tableRef, tableRef))
  }

  test("key requests throw an exception if table doesn't exist") {
    val client = headClient
    try {
      client.getPrimaryKeys(TableRef.of("default", "default", "testtable"))
      assert(false)
    } catch {
      case ex: Exception => assert(ex.getClass == CallStatus.NOT_FOUND.toRuntimeException.getClass)
    }
  }


  override def afterAll(): Unit = {
    producers.foreach(p => p.asInstanceOf[FlightSqlProducer].close())
    zkServer.close()
    servers.foreach(s => s.close())
  }

}
