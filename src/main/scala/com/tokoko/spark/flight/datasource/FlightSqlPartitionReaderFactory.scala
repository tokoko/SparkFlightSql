package com.tokoko.spark.flight.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class FlightSqlPartitionReaderFactory(flightInfoBuffer: Array[Byte]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val flightSqlPartition = partition.asInstanceOf[FlightSqlPartition]
    new FlightSqlPartitionReader(flightInfoBuffer, flightSqlPartition.endpointIndex)
  }
}
