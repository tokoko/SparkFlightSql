package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.{FlightClient, FlightInfo, Location}
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExtended

import java.nio.ByteBuffer

class FlightSqlPartitionReader(flightInfoBuffer: Array[Byte], endpointIndex: Int) extends PartitionReader[InternalRow]{
  var dataPulled = false
  val rootAllocator = new RootAllocator(Long.MaxValue)
  var iterator: Iterator[InternalRow] = _
  var flightClient: FlightClient = _

  private val flightInfo = FlightInfo.deserialize(ByteBuffer.wrap(flightInfoBuffer))

  override def next(): Boolean = {
    if (!dataPulled) {
      val endpoint = flightInfo.getEndpoints.get(endpointIndex)
      val clientLocation = endpoint.getLocations.get(0)

      flightClient = FlightClient.builder(rootAllocator, clientLocation).build

      val stream = flightClient.getStream(endpoint.getTicket)

      while (stream.next) {
        iterator = ArrowUtilsExtended.fromVectorSchemaRoot(stream.getRoot)
      }

      dataPulled = true
    }

    iterator.hasNext
  }

  override def get(): InternalRow = iterator.next()

  override def close(): Unit = {
//    flightClient.close()
  }
}
