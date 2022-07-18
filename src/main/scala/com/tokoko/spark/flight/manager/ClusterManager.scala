package com.tokoko.spark.flight.manager

import com.google.protobuf.ByteString
import org.apache.arrow.flight.{Action, FlightProducer, Location, Result}

object ClusterManager {
  def getClusterManager(conf: Map[String, String]): ClusterManager = {
    val mode = conf.getOrElse("spark.flight.manager", "static")

    if (mode == "static") new InMemoryClusterManager(conf)
    else if (mode == "zookeeper") new ZookeeperClusterManager(conf)
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
    val location = ClusterManager.getLocation(conf)

    NodeInfo(conf.getOrElse("spark.flight.internal.host", hostAddress)
      ,Integer.parseInt(conf.getOrElse("spark.flight.internal.port", location.getUri.getPort.toString))
      ,conf.getOrElse("spark.flight.public.host", hostAddress)
      ,Integer.parseInt(conf.getOrElse("spark.flight.public.port", location.getUri.getPort.toString)))
  }

}

trait ClusterManager extends AutoCloseable {

  def getLocation: Location

  def getPeers: List[NodeInfo]

  def getNodes: List[NodeInfo]

  def getInfo: NodeInfo

  def addFlight(handle: ByteString): Unit

  def setCompleted(handle: ByteString): Unit

  def getStatus(handle: ByteString): String

  def handleDoAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Boolean
    = false

}
