package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.FlightInfo
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType


class FlightSqlPartition(val endpointIndex: Int) extends InputPartition

class FlightSqlScan(schema: StructType, flightInfoBuffer: Array[Byte]) extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new FlightSqlBatch(flightInfoBuffer)

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
//    super.toMicroBatchStream(checkpointLocation)
    null
  }
}
