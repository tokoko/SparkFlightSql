package com.tokoko.spark.flight

import com.tokoko.spark.flight.manager.ClusterManager
import com.tokoko.spark.flight.utils.TestUtils
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight._
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.BigIntVector
import org.apache.curator.test.TestingServer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.util.ArrowHelpers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.Optional
import scala.collection.mutable
import collection.JavaConverters._


class SparkFlightSqlProducerDataSuite extends AnyFunSuite with BeforeAndAfterAll {

  var clients: Seq[FlightSqlClient] = _
  var servers: Seq[FlightServer] = _
  var rootAllocator: BufferAllocator = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    new TestingServer(9003, true)
    spark = SparkSession.builder
      .master("local")
      .enableHiveSupport
      .appName("SparkFlightSqlServer").getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    spark.range(10).toDF("id").write.mode("overwrite").saveAsTable("TestTable")

    rootAllocator = new RootAllocator(Long.MaxValue)

    val setup = TestUtils.startServers(rootAllocator, spark, Seq(9000, 9001), "basic", "static")

    servers = setup._1
    clients = setup._2

//    clients = servers.map(server => {
//      val clientLocation = Location.forGrpcInsecure("localhost", server.getPort)
//      val client = FlightClient.builder(rootAllocator, clientLocation).build
//      client.authenticateBasic("user", "password")
//      val flightSqlClient = new FlightSqlClient(client)
//      (flightSqlClient, client)
//    })

  }

  test("check select statement") {
    val query = "SELECT * FROM testtable"
    val fi = clients.head.execute(query)

    assert(TestUtils.assertSmallDataFrameEquality(
      TestUtils.toDf(fi, spark, rootAllocator).orderBy("id"), spark.sql(query)))
  }

  test("prepared statements throw an exception") {
    try {
      clients.head.prepare("SELECT * FROM testtable")
    } catch {
      case ex: Exception => assert(ex.getClass == CallStatus.UNIMPLEMENTED.toRuntimeException.getClass)
    }
  }

  test("update statements throw an exception") {
    try {
      clients.head.executeUpdate("UPDATE id = 1 FROM testtable")
    } catch {
      case ex: Exception => assert(ex.getClass == CallStatus.UNIMPLEMENTED.toRuntimeException.getClass)
    }
  }

  override def afterAll(): Unit = {
    servers.foreach(_.shutdown)
  }

}
