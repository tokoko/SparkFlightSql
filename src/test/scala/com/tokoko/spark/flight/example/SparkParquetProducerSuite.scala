package com.tokoko.spark.flight.example

import com.tokoko.spark.flight.manager.SparkFlightManager
import com.tokoko.spark.flight.utils.TestUtils
import org.apache.arrow.flight.{FlightClient, FlightDescriptor, FlightProducer, FlightServer}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.curator.test.TestingServer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite


class SparkParquetProducerSuite extends AnyFunSuite with BeforeAndAfterAll {

  var clients: Seq[FlightClient] = _
  var servers: Seq[FlightServer] = _
  var managers: Seq[SparkFlightManager] = _
  var rootAllocator: BufferAllocator = _
  var spark: SparkSession = _
  var zkServer: TestingServer = _

  override def beforeAll(): Unit = {
    val zookeeperPort = 9028
    zkServer = new TestingServer(zookeeperPort, true)
    spark = SparkSession.builder
      .master("local")
      .enableHiveSupport
      .appName("SparkFlightSqlServer").getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    spark.range(20).toDF("id")
      .write
      .format("parquet")
      .mode("overwrite")
      .save("local/test")

    rootAllocator = new RootAllocator(Long.MaxValue)

    val setup = TestUtils.startServersCommon(rootAllocator,
      spark,
      Seq(9024, 9026),
      "basic",
      "zookeeper",
      zookeeperPort.toString,
      "delta"
      )

    servers = setup._1
    clients = setup._2
    managers = setup._4
  }

  test("check select statement") {
    val df = spark.read.parquet("local/test")
    val fi = clients.head.getInfo(FlightDescriptor.path("local/test"))

    assert(TestUtils.assertSmallDataFrameEquality(
      TestUtils.toDf(fi, spark, rootAllocator).orderBy("id"), df))
  }

  override def afterAll(): Unit = {
    managers.foreach(_.close)
    servers.foreach(_.close)
    zkServer.close()
  }

}
