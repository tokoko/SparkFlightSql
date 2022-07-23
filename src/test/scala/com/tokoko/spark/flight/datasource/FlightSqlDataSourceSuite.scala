package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FlightSqlDataSourceSuite  extends AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var server: FlightServer = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .master("local")
      .enableHiveSupport
      .appName("SparkFlightSqlServer").getOrCreate

    val rootAllocator = new RootAllocator(Long.MaxValue)

    server = FlightServer.builder(rootAllocator, Location.forGrpcInsecure("localhost", 9000),
      new TestFlightSqlProducer()
    ).build()

    server.start
  }

  test("query test flight sql server") {
    val df = spark.read
        .format("com.tokoko.spark.flight.datasource")
        .option("url", "localhost:9000")
        .option("query", "select * from main.dbo.users")
        .load()

    assert(df.count() == 6)
  }

  override def afterAll(): Unit = {
    server.close()
  }

}
