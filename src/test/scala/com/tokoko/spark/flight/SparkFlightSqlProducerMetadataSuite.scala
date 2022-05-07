package com.tokoko.spark.flight

import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.sql.util.TableRef
import org.apache.arrow.flight.{CallStatus, FlightClient, FlightInfo, FlightRuntimeException, FlightServer, FlightStream, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, stats}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import scala.collection.mutable

class SparkFlightSqlProducerMetadataSuite extends AnyFunSuite with BeforeAndAfterAll {

  var client: FlightSqlClient = _
  var server: FlightServer = _

  override def beforeAll(): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .enableHiveSupport
      .appName("SparkFlightSqlServer").getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    spark.range(10).toDF("id").write.mode("overwrite").saveAsTable("TestTable")

    val rootAllocator = new RootAllocator(Long.MaxValue)
    val location = Location.forGrpcInsecure("localhost", 0)
    server = FlightServer.builder(rootAllocator, location,
      new SparkFlightSqlProducer(location, location, spark)).build

    server.start

    val clientLocation = Location.forGrpcInsecure("localhost", server.getPort)
    client = new FlightSqlClient(FlightClient.builder(rootAllocator, clientLocation).build)
  }

  // catalogs
  // TODO schemas -- same as catalogs ??
  // TODO tables
  // TODO tables - tableFilterPatterns
  // TODO tables - tableTypes
  // TODO tables - schemas
  // TODO table types
  // TODO sqlinfo
  // TODO check that table exists for keys
  // TODO check prepared statement goes to runtime exception

  test("getCatalogs returns databases") {
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

    assert(catalogs == mutable.Set("default"))
  }

  test("getTables returns all tables") {
    val fi = client.getTables("default", null, null, null, false)
    val stream = client.getStream(fi.getEndpoints.get(0).getTicket)

    val tables: mutable.Set[(String, String, String, String)] = mutable.Set.empty

    while (stream.next) {
      val root = stream.getRoot
      val catalogNameVector: VarCharVector = root.getVector("catalog_name").asInstanceOf[VarCharVector]
      val schemaNameVector: VarCharVector = root.getVector("db_schema_name").asInstanceOf[VarCharVector]
      val tableNameVector: VarCharVector = root.getVector("table_name").asInstanceOf[VarCharVector]
      val tableTypeVector: VarCharVector = root.getVector("table_type").asInstanceOf[VarCharVector]
      println(root.contentToTSVString())

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
      ("default", "default", "testtable", "MANAGED")
    ))
  }

  def assertEmpty(flightInfo: FlightInfo): Unit = {
    val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket)

    while (stream.next) {
      assert(stream.getRoot.getRowCount == 0)
    }
  }

  test("key requests are always empty") {
    val tableRef = TableRef.of("default", "default", "testtable")
    assertEmpty(client.getPrimaryKeys(tableRef))
    assertEmpty(client.getImportedKeys(tableRef))
    assertEmpty(client.getExportedKeys(tableRef))
    assertEmpty(client.getCrossReference(tableRef, tableRef))
  }

  // TODO
//  test("key requests throw an exception if table doesn't exist") {
//    val fi = client.getPrimaryKeys(TableRef.of("default", "default", "testtable"))
//  }


  override def afterAll(): Unit = {
    server.shutdown()
  }

}
