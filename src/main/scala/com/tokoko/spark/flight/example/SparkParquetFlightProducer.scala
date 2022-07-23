package com.tokoko.spark.flight.example

import com.tokoko.spark.flight.manager.SparkFlightManager
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkParquetFlightProducer {
  private val spark = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder.master("local").getOrCreate
  )

  private val rootAllocator = new RootAllocator(Long.MaxValue)
  private val manager = SparkFlightManager.getClusterManager(spark.conf.getAll)

  private val server = FlightServer.builder(rootAllocator, manager.getLocation,
    new SparkParquetFlightProducer(manager, spark)
  ).build()

  server.start()
  server.awaitTermination()
}

class SparkParquetFlightProducer(val clusterManager: SparkFlightManager, val spark: SparkSession) extends FlightProducer{
  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    clusterManager.streamDistributedFlight(ticket, listener)
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = ???

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val df = spark.read
      .format("parquet")
      .load(descriptor.getPath.asScala.toList:_*)

    clusterManager.distributeFlight(descriptor, df)
  }

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = ???

  override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = ???
}
