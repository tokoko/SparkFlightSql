package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.FlightInfo
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import collection.JavaConverters._
import java.nio.ByteBuffer

class FlightSqlPartition(val endpointIndex: Int) extends InputPartition

class FlightSqlScan(schema: StructType, flightInfoBuffer: Array[Byte]) extends Scan with Batch {
  private val flightInfo = FlightInfo.deserialize(ByteBuffer.wrap(flightInfoBuffer))

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    flightInfo.getEndpoints.asScala
      .zipWithIndex
      .map(e => new FlightSqlPartition(e._2)).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new FlightSqlPartitionReaderFactory(flightInfoBuffer)

  override def toBatch: Batch = this
}
