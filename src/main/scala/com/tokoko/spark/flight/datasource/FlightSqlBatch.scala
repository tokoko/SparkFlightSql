package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.FlightInfo
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import java.nio.ByteBuffer
import collection.JavaConverters._

class FlightSqlBatch(flightInfoBuffer: Array[Byte]) extends Batch {
  private val flightInfo = FlightInfo.deserialize(ByteBuffer.wrap(flightInfoBuffer))

  override def planInputPartitions(): Array[InputPartition] = {
    flightInfo.getEndpoints.asScala
      .zipWithIndex
      .map(e => new FlightSqlPartition(e._2)).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new FlightSqlPartitionReaderFactory(flightInfoBuffer)

}
