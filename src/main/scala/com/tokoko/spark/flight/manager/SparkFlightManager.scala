package com.tokoko.spark.flight.manager

import com.google.protobuf.ByteString
import org.apache.arrow.flight.{FlightDescriptor, FlightInfo, FlightProducer, Location, Ticket}
import org.apache.spark.sql.DataFrame

object SparkFlightManager {
  def getClusterManager(conf: Map[String, String]): SparkFlightManager = {
    val mode = conf.getOrElse("spark.flight.manager", "zookeeper")

//    if (mode == "static") new InMemoryClusterManager(conf)
    if (mode == "zookeeper") new ZookeeperSparkFlightManager(conf)
    else null
  }

  def getLocation(conf: Map[String, String]): Location = {
    Location.forGrpcInsecure(
      conf.getOrElse("spark.flight.host", "localhost"),
      Integer.parseInt(
        conf.getOrElse("spark.flight.port", "9000")
      )
    )
  }

  def getNodeInfo(conf: Map[String, String]): NodeInfo = {
    val hostAddress = java.net.InetAddress.getLocalHost.getHostAddress
    val location = SparkFlightManager.getLocation(conf)

    NodeInfo(conf.getOrElse("spark.flight.internal.host", hostAddress)
      ,Integer.parseInt(conf.getOrElse("spark.flight.internal.port", location.getUri.getPort.toString))
      ,conf.getOrElse("spark.flight.public.host", hostAddress)
      ,Integer.parseInt(conf.getOrElse("spark.flight.public.port", location.getUri.getPort.toString)))
  }

}

trait SparkFlightManager extends AutoCloseable {

  def getLocation: Location

  def getNodeInfo: NodeInfo

  def distributeFlight(descriptor: FlightDescriptor,
                       df: DataFrame,
                       ticketBuilder: Array[Byte] => Ticket = handle => new Ticket(handle)): FlightInfo

  def streamDistributedFlight(ticket: Ticket,
                              listener: FlightProducer.ServerStreamListener,
                              handleSelector: Ticket => Array[Byte] = ticket => ticket.getBytes): Unit

  def streamDistributedFlight(handle: ByteString, listener: FlightProducer.ServerStreamListener): Unit

}
