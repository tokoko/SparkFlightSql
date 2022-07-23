package com.tokoko.spark.flight.datasource

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class FlightSqlScanBuilder(schema: StructType, flightInfoBuffer: Array[Byte]) extends ScanBuilder {
  override def build(): Scan = new FlightSqlScan(schema, flightInfoBuffer)
}
