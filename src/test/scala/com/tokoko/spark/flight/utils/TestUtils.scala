package com.tokoko.spark.flight.utils

import com.tokoko.spark.flight.SparkFlightSqlProducer
import com.tokoko.spark.flight.manager.ClusterManager
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.{FlightClient, FlightInfo, FlightServer}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.util.ArrowHelpers
import org.apache.spark.sql.{DataFrame, SparkSession}
import collection.JavaConverters._
import scala.collection.mutable

object TestUtils {

  def startServers(allocator: BufferAllocator, spark: SparkSession, serverPorts: Seq[Int]): Seq[FlightServer] = {
    val managers = serverPorts.map(port => {
      ClusterManager.getClusterManager(Map(
        "spark.flight.host" -> "localhost",
        "spark.flight.port" -> port.toString,
        "spark.flight.internal.host" -> "localhost",
        "spark.flight.internal.port" -> port.toString,
        "spark.flight.public.host" -> "localhost",
        "spark.flight.public.port" -> port.toString,
        "spark.flight.manager" -> "static",
        "spark.flight.manager.static.peers" -> {
          serverPorts.filter(p => p != port)
            .map(p => s"localhost:$p,localhost:$p")
            .mkString(";")
        }
      ))
    })

    val servers = managers.map(manager => {
      FlightServer.builder(allocator, manager.getLocation, new SparkFlightSqlProducer(manager, spark))
        .build
    })

    servers.foreach(_.start)
    servers
  }

  def startServersZookeeper(allocator: BufferAllocator, spark: SparkSession, serverPorts: Seq[Int]): Seq[FlightServer] = {
    val managers = serverPorts.map(port => {
      ClusterManager.getClusterManager(Map(
        "spark.flight.host" -> "localhost",
        "spark.flight.port" -> port.toString,
        "spark.flight.internal.host" -> "localhost",
        "spark.flight.internal.port" -> port.toString,
        "spark.flight.public.host" -> "localhost",
        "spark.flight.public.port" -> port.toString,
        "spark.flight.manager" -> "zookeeper",
        "spark.flight.manager.zookeeper.url" -> "localhost:9003",
        "spark.flight.manager.zookeeper.membershipPath" -> "/spark-flight-sql"
      ))
    })

    Thread.sleep(2000)

    val servers = managers.map(manager => {
      FlightServer.builder(allocator, manager.getLocation, new SparkFlightSqlProducer(manager, spark))
        .build
    })

    servers.foreach(_.start)
    servers
  }

  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Boolean = {
    if (!actualDF.schema.equals(expectedDF.schema)) {
      return false
    }
    if (!actualDF.collect().sameElements(expectedDF.collect())) {
      return false
    }
    true
  }

  def toDf(flightInfo: FlightInfo, spark: SparkSession, rootAllocator: BufferAllocator): DataFrame = {
    val dfs: mutable.Set[(DataFrame)] = mutable.Set.empty

    flightInfo.getEndpoints.asScala.foreach(endpoint => {
      val client = new FlightSqlClient(FlightClient.builder(rootAllocator, endpoint.getLocations.get(0)).build)
      val stream = client.getStream(endpoint.getTicket)

      while (stream.next) {
        dfs.add(ArrowHelpers.toDataFrame(spark, stream.getRoot))
      }

    })

    dfs.reduceLeft((a, b) => a.union(b))
  }

}
