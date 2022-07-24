package com.tokoko.spark.flight.sql

import com.tokoko.spark.flight.auth.AuthHandler
import com.tokoko.spark.flight.manager.SparkFlightManager
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession

object SparkFlightSqlServer extends App {

  val spark = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder.master("local").getOrCreate
  )

//  // TODO temporary for testing
//  if (
//    spark.conf.get("spark.flight.public.port", "") == "9001"
//  ) {
//    spark.range(17).toDF("id").write.mode("overwrite").saveAsTable("TestTable")
//    spark.sql("REFRESH TABLE TestTable").show()
//  }

  val rootAllocator = new RootAllocator(Long.MaxValue)

  val manager = SparkFlightManager.getClusterManager(spark.conf.getAll)

  val server = FlightServer.builder(rootAllocator, manager.getLocation, new SparkFlightSqlProducer(manager, spark))
    .authHandler(AuthHandler(spark.conf.getAll))
    .build

  server.start

  println("Server Running at ")
  server.awaitTermination()
}
