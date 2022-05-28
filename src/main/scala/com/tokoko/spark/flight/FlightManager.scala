package com.tokoko.spark.flight

import com.google.protobuf.ByteString
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import java.util

class FlightManager(val locations: List[Location], val allocator: BufferAllocator) {

  private val flights = new util.HashMap[ByteString, String]

  private val clients = locations
    .map((loc: Location) => Location.forGrpcInsecure(loc.getUri.getHost, loc.getUri.getPort))
    .map((location: Location) => FlightClient.builder(allocator, location).build)


  def addFlight(handle: ByteString): Unit = {
    if (flights.containsKey(handle)) throw CallStatus.ALREADY_EXISTS.toRuntimeException
    flights.put(handle, "RUNNING")
  }

  def setCompleted(handle: ByteString): Unit = {
    if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
    flights.put(handle, "COMPLETED")
  }

  def getStatus(handle: ByteString): String = {
    if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
    flights.get(handle)
  }

  def broadcast(handle: ByteString): Unit = {
    if (!flights.containsKey(handle)) throw new RuntimeException("Flight doesn't exist")
    val action = new Action(flights.get(handle), handle.toByteArray)

    clients.foreach((client: FlightClient) => {
      val it = client.doAction(action)
      while (it.hasNext) println(it.next.getBody.length)
    })
  }
}
