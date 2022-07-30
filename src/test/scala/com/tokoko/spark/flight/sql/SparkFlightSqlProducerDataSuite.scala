package com.tokoko.spark.flight.sql

import com.tokoko.spark.flight.utils.TestUtils
import org.apache.arrow.flight._
import org.apache.arrow.flight.sql.{FlightSqlClient, FlightSqlProducer}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.curator.test.TestingServer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite


class SparkFlightSqlProducerDataSuite extends AnyFunSuite with BeforeAndAfterAll {

  var clients: Seq[FlightClient] = _
  var servers: Seq[FlightServer] = _
  var producers: Seq[FlightProducer] = _
  var rootAllocator: BufferAllocator = _
  var spark: SparkSession = _
  var zkServer: TestingServer = _
  var headClient: FlightSqlClient = _

  override def beforeAll(): Unit = {
    val zookeeperPort = 9008
    zkServer = new TestingServer(zookeeperPort, true)
    spark = SparkSession.builder
      .master("local")
      .enableHiveSupport
      .appName("SparkFlightSqlServer").getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    spark.range(10).toDF("id").write.mode("overwrite").saveAsTable("TestTable")

    rootAllocator = new RootAllocator(Long.MaxValue)

    val setup = TestUtils.startServersCommon(rootAllocator, spark, Seq(9004, 9006), "basic", "zookeeper", zookeeperPort.toString, "sql")

    servers = setup._1
    clients = setup._2
    producers = setup._3

    headClient = new FlightSqlClient(clients.head)
  }

  test("check select statement") {
    val query = "SELECT * FROM testtable"
    val fi = headClient.execute(query)

    assert(TestUtils.assertSmallDataFrameEquality(
      TestUtils.toDf(fi, spark, rootAllocator).orderBy("id"), spark.sql(query)))
  }

  test("prepared statements throw an exception") {
    try {
      headClient.prepare("SELECT * FROM testtable")
    } catch {
      case ex: Exception => assert(ex.getClass == CallStatus.UNIMPLEMENTED.toRuntimeException.getClass)
    }
  }

  test("update statements throw an exception") {
    try {
      headClient.executeUpdate("UPDATE id = 1 FROM testtable")
    } catch {
      case ex: Exception => assert(ex.getClass == CallStatus.UNIMPLEMENTED.toRuntimeException.getClass)
    }
  }

  override def afterAll(): Unit = {
    producers.foreach(p => p.asInstanceOf[FlightSqlProducer].close())
    servers.foreach(_.close)
    zkServer.close()
  }

}
