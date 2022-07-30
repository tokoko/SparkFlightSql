package com.tokoko.spark.flight.datasource

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import collection.JavaConverters._
import java.util

class FlightSqlTable(schema: StructType, flightInfoBuffer: Array[Byte]) extends Table with SupportsRead {
  override def name(): String = ""

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

//  TableCapability.MICRO_BATCH_READ

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new FlightSqlScanBuilder(schema, flightInfoBuffer)
  }
}
