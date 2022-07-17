package com.tokoko.spark.flight

import com.tokoko.spark.flight.manager.ClusterManager
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession

object SparkFlightSqlServer extends App {

  val spark = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder.master("local").getOrCreate
  )

  // TODO temporary for testing
  if (
    spark.conf.get("spark.flight.public.port", "") == "9001"
  ) {
    spark.range(17).toDF("id").write.mode("overwrite").saveAsTable("TestTable")
    spark.sql("REFRESH TABLE TestTable").show()
  }

  val rootAllocator = new RootAllocator(Long.MaxValue)

  val manager = ClusterManager.getClusterManager(spark.conf.getAll)

  val server = FlightServer.builder(rootAllocator, manager.getLocation,
    new SparkFlightSqlProducer(manager, spark)
  ).build

  server.start

  println("Server Running at ")
  server.awaitTermination()
}
