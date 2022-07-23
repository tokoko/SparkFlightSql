package com.tokoko.spark.flight.datasource

import org.apache.arrow.flight.{FlightClient, FlightInfo, Location}
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowUtilsExtended, CaseInsensitiveStringMap}

class DefaultSource extends TableProvider {

  private var flightInfo: FlightInfo = _

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    val rootAllocator = new RootAllocator()
    val hostname = caseInsensitiveStringMap.get("url").split(":").head
    val port = Integer.parseInt(caseInsensitiveStringMap.get("url").split(":").last)
    val clientLocation = Location.forGrpcInsecure(hostname, port)
    val client = FlightClient.builder(rootAllocator, clientLocation).build
    val flightSqlClient = new FlightSqlClient(client)
    flightInfo = flightSqlClient.execute(caseInsensitiveStringMap.get("query"))
    val res = ArrowUtilsExtended.fromArrowSchema(flightInfo.getSchema)
    flightSqlClient.close()
    res
  }

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: java.util.Map[String, String]): Table =
    new FlightSqlTable(schema, flightInfo.serialize().array())
}
