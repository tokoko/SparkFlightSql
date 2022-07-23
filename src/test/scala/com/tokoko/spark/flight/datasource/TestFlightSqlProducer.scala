package com.tokoko.spark.flight.datasource

import com.google.protobuf.Any.pack
import com.google.protobuf.ByteString.copyFrom
import com.google.protobuf.{ByteString, Message}
import org.apache.arrow.flight.{Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, FlightProducer, FlightStream, Location, PutResult, Result, SchemaResult, Ticket}
import org.apache.arrow.flight.sql.FlightSqlProducer
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.nio.charset.StandardCharsets
import collection.JavaConverters._
import java.util.Arrays.asList
import java.util.UUID.randomUUID
import scala.collection.mutable

/*_*/
class TestFlightSqlProducer extends FlightSqlProducer {
  /*_*/

  private def createTestRoot(): VectorSchemaRoot = {
        val schema = new Schema(
          asList(
            new Field("name", FieldType.nullable(new ArrowType.Utf8), null),
            new Field("birthYear", FieldType.nullable(new ArrowType.Int(32, true)
            ), null)))

        val allocator = new RootAllocator(Long.MaxValue)

        val table = VectorSchemaRoot.create(schema, allocator)

        val nameVector = table.getVector(0).asInstanceOf[VarCharVector]
        nameVector.allocateNew(3)
        nameVector.set(0, "Tatia".getBytes)
        nameVector.set(1, "Tato".getBytes)
        nameVector.set(2, "Tornike".getBytes)
        nameVector.setValueCount(3)

        val birthVector = table.getVector(1).asInstanceOf[IntVector]
        birthVector.allocateNew(3)
        birthVector.set(0, 1992)
        birthVector.set(1, 1995)
        birthVector.set(2, 1996)
        birthVector.setValueCount(3)

        table.setRowCount(3)
        table
      }

  val rootAllocator = new RootAllocator()

  private val handleQueries = new mutable.HashMap[ByteString, String]()

  private val testData = Map(
    "select * from main.dbo.users" -> createTestRoot()
  )

  private def getFlightInfoForSchema(schema: Schema, request: Message, descriptor: FlightDescriptor): FlightInfo = {
    val location = Location.forGrpcInsecure("localhost", 9000)
    val ticket: Ticket = new Ticket(pack(request).toByteArray)
    val endpoints = List(new FlightEndpoint(ticket, location))
    new FlightInfo(schema, descriptor, endpoints.asJava, -1, -1)
  }

  def populateVarCharVector(vector: VarCharVector, data: List[String]): Unit = {
    vector.allocateNew(data.size)

    data.zipWithIndex
      .foreach(row => vector.set(row._2, row._1.getBytes))

    vector.setValueCount(data.size)
  }
  /*_*/
  override def createPreparedStatement(request: FlightSql.ActionCreatePreparedStatementRequest, context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[Result]): Unit = ???

  override def closePreparedStatement(request: FlightSql.ActionClosePreparedStatementRequest, context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[Result]): Unit = ???

  override def getFlightInfoStatement(command: FlightSql.CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {

    val table = testData(command.getQuery)
    val handle = copyFrom(randomUUID.toString.getBytes(StandardCharsets.UTF_8))
    handleQueries.put(handle, command.getQuery)
    val ticketStatementQuery = FlightSql.TicketStatementQuery.newBuilder.setStatementHandle(handle).build
    val ticket = new Ticket(pack(ticketStatementQuery).toByteArray)
    val location = Location.forGrpcInsecure("localhost", 9000)

    val endpoints = List(new FlightEndpoint(ticket, location), new FlightEndpoint(ticket, location))
    new FlightInfo(table.getSchema, descriptor, endpoints.asJava, -1, -1)
  }

  override def getStreamStatement(ticket: FlightSql.TicketStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    val query = handleQueries(ticket.getStatementHandle)
    val table = testData(query)
    listener.start(table)
    listener.putNext()
    listener.completed()
  }

  override def getFlightInfoPreparedStatement(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getSchemaStatement(command: FlightSql.CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): SchemaResult = ???

  override def getStreamPreparedStatement(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def acceptPutStatement(command: FlightSql.CommandStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def acceptPutPreparedStatementUpdate(command: FlightSql.CommandPreparedStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def acceptPutPreparedStatementQuery(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def getFlightInfoSqlInfo(request: FlightSql.CommandGetSqlInfo, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamSqlInfo(command: FlightSql.CommandGetSqlInfo, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getFlightInfoCatalogs(request: FlightSql.CommandGetCatalogs, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    getFlightInfoForSchema(Schemas.GET_CATALOGS_SCHEMA, request, descriptor)
  }

  override def getStreamCatalogs(context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    val table = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, rootAllocator)
    val catalogs = List("main", "secondary")
    populateVarCharVector(table.getVector("catalog_name").asInstanceOf[VarCharVector], catalogs)
    table.setRowCount(catalogs.size)
    listener.start(table)
    listener.putNext()
    listener.completed()
  }

  override def getFlightInfoSchemas(request: FlightSql.CommandGetDbSchemas, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamSchemas(command: FlightSql.CommandGetDbSchemas, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getFlightInfoTables(request: FlightSql.CommandGetTables, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamTables(command: FlightSql.CommandGetTables, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getFlightInfoTableTypes(request: FlightSql.CommandGetTableTypes, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamTableTypes(context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getFlightInfoPrimaryKeys(request: FlightSql.CommandGetPrimaryKeys, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamPrimaryKeys(command: FlightSql.CommandGetPrimaryKeys, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getFlightInfoExportedKeys(request: FlightSql.CommandGetExportedKeys, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getFlightInfoImportedKeys(request: FlightSql.CommandGetImportedKeys, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getFlightInfoCrossReference(request: FlightSql.CommandGetCrossReference, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamExportedKeys(command: FlightSql.CommandGetExportedKeys, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getStreamImportedKeys(command: FlightSql.CommandGetImportedKeys, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getStreamCrossReference(command: FlightSql.CommandGetCrossReference, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def close(): Unit = ???

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = ???
}
