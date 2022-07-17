package com.tokoko.spark.flight

import com.tokoko.spark.flight.utils.TestUtils
import org.apache.arrow.flight._
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.curator.test.TestingServer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite


class ZookeeperDataSuite extends AnyFunSuite with BeforeAndAfterAll {

  var clients: Seq[FlightSqlClient] = _
  var servers: Seq[FlightServer] = _
  var rootAllocator: BufferAllocator = _
  var spark: SparkSession = _
  var zkServer: TestingServer = _

  override def beforeAll(): Unit = {
    zkServer = new TestingServer(9003, true)
    spark = SparkSession.builder
      .master("local")
      .enableHiveSupport
      .appName("SparkFlightSqlServer").getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    spark.range(10).toDF("id").write.mode("overwrite").saveAsTable("TestTable")

    rootAllocator = new RootAllocator(Long.MaxValue)

    servers = TestUtils.startServersZookeeper(rootAllocator, spark, Seq(9000, 9001))

    clients = servers.map(server => {
      val clientLocation = Location.forGrpcInsecure("localhost", server.getPort)
      new FlightSqlClient(FlightClient.builder(rootAllocator, clientLocation).build)
    })

  }

  test("check select statement") {
    val query = "SELECT * FROM testtable"
    val fi = clients.head.execute(query)

    assert(TestUtils.assertSmallDataFrameEquality(
      TestUtils.toDf(fi, spark, rootAllocator).orderBy("id"), spark.sql(query)))
  }

  override def afterAll(): Unit = {
    servers.foreach(_.shutdown)
    zkServer.close()
  }

}
