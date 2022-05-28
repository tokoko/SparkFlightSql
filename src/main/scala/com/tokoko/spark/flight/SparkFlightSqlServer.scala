package com.tokoko.spark.flight

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

  val location = Location.forGrpcInsecure(
    spark.conf.get("spark.flight.host", "localhost"),
    Integer.parseInt(
      spark.conf.get("spark.flight.port", "9000")
    )
  )

  val hostAddress = java.net.InetAddress.getLocalHost.getHostAddress

  val internalLocation = Location.forGrpcInsecure(
    spark.conf.get("spark.flight.internal.host", hostAddress),
    Integer.parseInt(
      spark.conf.get("spark.flight.internal.port", location.getUri.getPort.toString)
    )
  )

  val publicLocation = Location.forGrpcInsecure(
    spark.conf.get("spark.flight.public.host", hostAddress),
    Integer.parseInt(
      spark.conf.get("spark.flight.public.port", location.getUri.getPort.toString)
    )
  )

  val peersConf = spark.conf.getOption("spark.flight.peers")

  val peerLocations = peersConf.map(
    conf => conf.split(";")
      .map(peer => {
        val adresses = peer.split(",").map(part => {
          Location.forGrpcInsecure(
            part.split(":")(0),
            Integer.parseInt(part.split(":")(1))
          )
        })

        if (adresses.length == 1) (adresses(0), adresses(0))
        else (adresses(0), adresses(1))

      })
  ).getOrElse(Array.empty)


  val server = FlightServer.builder(rootAllocator, location,
    new SparkFlightSqlProducer(internalLocation, publicLocation, spark, peerLocations)
  ).build

  server.start

  println("Server Running at " + internalLocation)
  server.awaitTermination()
}
