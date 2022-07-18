package com.tokoko.spark.flight

import com.tokoko.spark.flight.manager.ClusterManager
import org.apache.arrow.flight.auth.BasicServerAuthHandler
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession

import java.util.Optional
import scala.collection.mutable
import scala.util.Random

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

  val builder = FlightServer.builder(rootAllocator, manager.getLocation,
    new SparkFlightSqlProducer(manager, spark)
  )

  val server = builder
//      .authHandler(new BasicServerAuthHandler(
//        new BasicServerAuthHandler.BasicAuthValidator {
//          private val validTokens = new mutable.HashSet[Array[Byte]]()
//
//          def getToken(username: String, password: String): Array[Byte] = {
//            if (!(username.equals("test") && password.equals("test"))) {
//              throw new Exception("Invalid Auth")
//            }
//
//            val b = new Array[Byte](20)
//            Random.nextBytes(b)
//            validTokens.add(b)
//            b
//          }
//
//          def isValid(token: Array[Byte]): Optional[String] = {
//            Optional.empty()
////            if (validTokens.contains(token)) {
////              Optional.of("test")
////            } else {
////              Optional.empty()
////            }
//          }
//        }
//      ))
    .build

  server.start

  println("Server Running at ")
  server.awaitTermination()
}
