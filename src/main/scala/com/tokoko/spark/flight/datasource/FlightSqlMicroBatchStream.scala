package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.FlightInfo
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

class FlightSqlMicroBatchStream(flightInfoBuffer: Array[Byte]) extends MicroBatchStream {
  override def latestOffset(): Offset = ???

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = ???

  override def createReaderFactory(): PartitionReaderFactory = ???

  override def initialOffset(): Offset = ???

  override def deserializeOffset(json: String): Offset = ???

  override def commit(end: Offset): Unit = ???

  override def stop(): Unit = ???
}
