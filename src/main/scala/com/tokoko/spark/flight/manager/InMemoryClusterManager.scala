package com.tokoko.spark.flight.manager

import com.google.protobuf.ByteString
import org.apache.arrow.flight.{Action, CallStatus, FlightClient, FlightProducer, Location, Result}
import org.apache.arrow.memory.RootAllocator
import org.apache.log4j.Logger

import java.util

class InMemoryClusterManager(conf: Map[String, String]) extends ClusterManager {
  private val logger = Logger.getLogger(this.getClass)

  private val allocator = new RootAllocator()

  private val peerLocations = conf.get("spark.flight.manager.static.peers").map(
    conf => conf.split(";")
      .map(peer => {
        val addresses = peer.split(",").map(part => {
          (
            part.split(":")(0),
            Integer.parseInt(part.split(":")(1))
          )
        })

        if (addresses.length == 1) NodeInfo(addresses(0)._1, addresses(0)._2, addresses(0)._1, addresses(0)._2)
        else NodeInfo(addresses(0)._1, addresses(0)._2, addresses(1)._1, addresses(1)._2)

      })
  ).getOrElse(Array.empty)

  private val flights = new util.HashMap[ByteString, String]

  private val clients = peerLocations
    .map(_.internalLocation)
    .map((loc: Location) => Location.forGrpcInsecure(loc.getUri.getHost, loc.getUri.getPort))
    .map((location: Location) => FlightClient.builder(allocator, location).build)

  var authenticated = false

  override def getLocation: Location = ClusterManager.getLocation(conf)

  override def getPeers: List[NodeInfo] = peerLocations.toList

  override def getNodes: List[NodeInfo] = peerLocations.union(Array(getInfo)).toList

  override def getInfo: NodeInfo = ClusterManager.getNodeInfo(conf)

  override def addFlight(handle: ByteString): Unit = {
    if (flights.containsKey(handle)) throw CallStatus.ALREADY_EXISTS.toRuntimeException
    flights.put(handle, "RUNNING")
    broadcast(handle)
  }

  override def setCompleted(handle: ByteString): Unit = {
    if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
    flights.put(handle, "COMPLETED")
    broadcast(handle)
  }

  override def getStatus(handle: ByteString): String = {
    if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
    flights.get(handle)
  }

  private def broadcast(handle: ByteString): Unit = {
    logger.warn("Broadcasting changes")
    if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
    val action = new Action(flights.get(handle), handle.toByteArray)

    if (!authenticated) {
      clients.foreach(c => c.authenticateBasic("user", "password"))
      authenticated = true
    }

    clients.foreach((client: FlightClient) => {
      val it = client.doAction(action)
      while (it.hasNext) println(it.next.getBody.length)
    })
  }

  override def handleDoAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Boolean = {
    if (action.getType == "RUNNING") {
      val handle = ByteString.copyFrom(action.getBody)
      if (flights.containsKey(handle)) throw CallStatus.ALREADY_EXISTS.toRuntimeException
      flights.put(handle, "RUNNING")
      listener.onCompleted()
      true
    }
    else if (action.getType == "COMPLETED") {
      val handle = ByteString.copyFrom(action.getBody)
      if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
      flights.put(handle, "COMPLETED")
      listener.onCompleted()
      true
    } else false
  }

  override def close(): Unit = {}
}
