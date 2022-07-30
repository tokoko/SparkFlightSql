package com.tokoko.spark.flight.sql

import com.google.protobuf.Any.pack
import com.google.protobuf.ByteString.copyFrom
import com.google.protobuf.Message
import com.tokoko.spark.flight.manager.SparkFlightManager
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.flight.sql.{FlightSqlProducer, SqlInfoBuilder}
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.ArrowUtilsExtended

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import scala.collection.JavaConverters._


class SparkFlightSqlProducer(val clusterManager: SparkFlightManager, val spark: SparkSession) extends FlightSqlProducer {

  private val logger = Logger.getLogger(this.getClass)

  val rootAllocator = new RootAllocator()

  private val sqlInfoBuilder = new SqlInfoBuilder()
    .withFlightSqlServerName("SparkFlightSql")
    .withFlightSqlServerVersion("3.2.1")
    .withFlightSqlServerArrowVersion("7.0.0")
    .withFlightSqlServerReadOnly(true)

  private def getSingleEndpointFlightInfoForSchema(schema: Schema, request: Message, descriptor: FlightDescriptor): FlightInfo = {
    val location = clusterManager.getNodeInfo.publicLocation
    val ticket: Ticket = new Ticket(pack(request).toByteArray)
    val endpoints = List(new FlightEndpoint(ticket, location))
    new FlightInfo(schema, descriptor, endpoints.asJava, -1, -1)
  }

  private def emptyResponseForSchema(schema: Schema, listener: FlightProducer.ServerStreamListener): Unit = {
    val table = VectorSchemaRoot.create(schema, rootAllocator)
    table.setRowCount(0)
    listener.start(table)
    listener.putNext()
    listener.completed()
  }

  def populateVarCharVector(vector: VarCharVector, data: List[String]): Unit = {
    vector.allocateNew(data.size)

    data.zipWithIndex
      .foreach(row => vector.set(row._2, row._1.getBytes))

    vector.setValueCount(data.size)
  }

  def populateVarBinaryVector(vector: VarBinaryVector, data: List[Array[Byte]]): Unit = {
    vector.allocateNew(data.size)

    data.zipWithIndex
      .foreach(row => vector.set(row._2, row._1))

    vector.setValueCount(data.size)
  }

  override def createPreparedStatement(request: FlightSql.ActionCreatePreparedStatementRequest,
                                       context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[Result]): Unit = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def closePreparedStatement(request: FlightSql.ActionClosePreparedStatementRequest, context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[Result]): Unit = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def getFlightInfoStatement(command: FlightSql.CommandStatementQuery,
                                      context: FlightProducer.CallContext,
                                      descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoStatement")
    val query = command.getQuery
    val df = spark.sql(query).repartition(3)
    clusterManager.distributeFlight(descriptor, df, handle => {
      val ticketStatementQuery = FlightSql.TicketStatementQuery.newBuilder.setStatementHandle(copyFrom(handle)).build
      new Ticket(pack(ticketStatementQuery).toByteArray)
    })
  }

  override def getStreamStatement(ticket: FlightSql.TicketStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamStatement")
    val handle = ticket.getStatementHandle
    clusterManager.streamDistributedFlight(handle, listener)
  }


  override def getFlightInfoPreparedStatement(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def getSchemaStatement(command: FlightSql.CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): SchemaResult = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def getStreamPreparedStatement(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def acceptPutStatement(command: FlightSql.CommandStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def acceptPutPreparedStatementUpdate(command: FlightSql.CommandPreparedStatementUpdate, context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def acceptPutPreparedStatementQuery(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def getFlightInfoSqlInfo(request: FlightSql.CommandGetSqlInfo, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoSqlInfo")
    getSingleEndpointFlightInfoForSchema(Schemas.GET_SQL_INFO_SCHEMA, request, descriptor)
  }

  override def getStreamSqlInfo(command: FlightSql.CommandGetSqlInfo, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamSqlInfo")
    sqlInfoBuilder.send(command.getInfoList, listener)
  }

  override def getFlightInfoCatalogs(request: FlightSql.CommandGetCatalogs,
                                     context: FlightProducer.CallContext,
                                     descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoCatalogs")
    getSingleEndpointFlightInfoForSchema(Schemas.GET_CATALOGS_SCHEMA, request, descriptor)
  }

  override def getStreamCatalogs(context: FlightProducer.CallContext,
                                 listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamCatalogs")
    val table = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, rootAllocator)
    val catalogs = CatalogUtils.listCatalogs(spark)
    populateVarCharVector(table.getVector("catalog_name").asInstanceOf[VarCharVector], catalogs)
    table.setRowCount(catalogs.size)
    listener.start(table)
    listener.putNext()
    listener.completed()
  }

  override def getFlightInfoSchemas(request: FlightSql.CommandGetDbSchemas,
                                    context: FlightProducer.CallContext,
                                    descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoSchemas")
    getSingleEndpointFlightInfoForSchema(Schemas.GET_SCHEMAS_SCHEMA, request, descriptor)
  }

  override def getStreamSchemas(command: FlightSql.CommandGetDbSchemas,
                                context: FlightProducer.CallContext,
                                listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamSchemas")
    val catalog = command.getCatalog
    val filterPattern = if (command.hasDbSchemaFilterPattern) command.getDbSchemaFilterPattern else null
    val table = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, rootAllocator)
    val schemas = CatalogUtils.listNamespaces(spark, catalog, filterPattern)
    populateVarCharVector(table.getVector("catalog_name").asInstanceOf[VarCharVector], schemas.map(_._1))
    populateVarCharVector(table.getVector("db_schema_name").asInstanceOf[VarCharVector], schemas.map(_._2))

    table.setRowCount(schemas.size)
    listener.start(table)
    listener.putNext()
    listener.completed()
  }

  override def getFlightInfoTables(request: FlightSql.CommandGetTables,
                                   context: FlightProducer.CallContext,
                                   descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoTables")
    val schema = if (request.getIncludeSchema) Schemas.GET_TABLES_SCHEMA else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
    getSingleEndpointFlightInfoForSchema(schema, request, descriptor)
  }

  override def getStreamTables(command: FlightSql.CommandGetTables, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamTables")
    val schema = if (command.getIncludeSchema) Schemas.GET_TABLES_SCHEMA else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
    val catalog = command.getCatalog
    val schemaPattern = if (command.hasDbSchemaFilterPattern) command.getDbSchemaFilterPattern else null
    val tablePattern = if (command.hasTableNameFilterPattern) command.getTableNameFilterPattern else null
    //        command.getTableTypesList().asByteStringList().
    val includeSchema: Boolean = command.getIncludeSchema

    val tables = CatalogUtils.listTables(spark, catalog, schemaPattern, tablePattern)
    val res: VectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)

    populateVarCharVector(res.getVector("table_name").asInstanceOf[VarCharVector], tables.map(_._3))
    populateVarCharVector(res.getVector("db_schema_name").asInstanceOf[VarCharVector], tables.map(_._2))
    populateVarCharVector(res.getVector("catalog_name").asInstanceOf[VarCharVector], tables.map(_._1))
    populateVarCharVector(res.getVector("table_type").asInstanceOf[VarCharVector], tables.map(_ => "MANAGED"))

    if (includeSchema) {
      populateVarBinaryVector(
        res.getVector("table_schema").asInstanceOf[VarBinaryVector],
        tables.map(table => {
          val sparkSchema = CatalogUtils.tableSchema(spark, table._1, table._2, table._3)
          val arrowSchema: Schema = ArrowUtilsExtended.toArrowSchema(sparkSchema, spark.sessionState.conf.sessionLocalTimeZone)
          val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream
          try MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), arrowSchema)
          catch {
            case e: IOException => e.printStackTrace()
          }
          val buffer: ByteBuffer = ByteBuffer.wrap(outputStream.toByteArray)

          copyFrom(buffer).toByteArray
        })
      )
    }

    res.setRowCount(tables.size)
    listener.start(res)
    listener.putNext()
    listener.completed()
  }

  override def getFlightInfoTableTypes(request: FlightSql.CommandGetTableTypes, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamTableTypes(context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???

  override def getFlightInfoPrimaryKeys(request: FlightSql.CommandGetPrimaryKeys,
                                        context: FlightProducer.CallContext,
                                        descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoPrimaryKeys")
    if (!CatalogUtils.tableExists(spark, request.getCatalog, request.getDbSchema, request.getTable))
      throw CallStatus.NOT_FOUND.toRuntimeException

    getSingleEndpointFlightInfoForSchema(Schemas.GET_PRIMARY_KEYS_SCHEMA, request, descriptor)
  }

  override def getFlightInfoExportedKeys(request: FlightSql.CommandGetExportedKeys,
                                         context: FlightProducer.CallContext,
                                         descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoExportedKeys")
    if (!CatalogUtils.tableExists(spark, request.getCatalog, request.getDbSchema, request.getTable))
      throw CallStatus.NOT_FOUND.toRuntimeException

    getSingleEndpointFlightInfoForSchema(Schemas.GET_EXPORTED_KEYS_SCHEMA, request, descriptor)
  }

  override def getFlightInfoImportedKeys(request: FlightSql.CommandGetImportedKeys,
                                         context: FlightProducer.CallContext,
                                         descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoImportedKeys")
    if (!CatalogUtils.tableExists(spark, request.getCatalog, request.getDbSchema, request.getTable))
      throw CallStatus.NOT_FOUND.toRuntimeException

    getSingleEndpointFlightInfoForSchema(Schemas.GET_IMPORTED_KEYS_SCHEMA, request, descriptor)
  }

  override def getFlightInfoCrossReference(request: FlightSql.CommandGetCrossReference,
                                           context: FlightProducer.CallContext,
                                           descriptor: FlightDescriptor): FlightInfo = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getFlightInfoCrossReference")
    if (
      !CatalogUtils.tableExists(spark, request.getPkCatalog, request.getPkDbSchema, request.getPkTable) ||
        !CatalogUtils.tableExists(spark, request.getFkCatalog, request.getFkDbSchema, request.getFkTable)
    ) throw CallStatus.NOT_FOUND.toRuntimeException

    getSingleEndpointFlightInfoForSchema(Schemas.GET_CROSS_REFERENCE_SCHEMA, request, descriptor)
  }

  override def getStreamPrimaryKeys(command: FlightSql.CommandGetPrimaryKeys,
                                    context: FlightProducer.CallContext,
                                    listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamPrimaryKeys")
    emptyResponseForSchema(Schemas.GET_PRIMARY_KEYS_SCHEMA, listener)
  }

  override def getStreamExportedKeys(command: FlightSql.CommandGetExportedKeys,
                                     context: FlightProducer.CallContext,
                                     listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamExportedKeys")
    emptyResponseForSchema(Schemas.GET_EXPORTED_KEYS_SCHEMA, listener)
  }

  override def getStreamImportedKeys(command: FlightSql.CommandGetImportedKeys,
                                     context: FlightProducer.CallContext,
                                     listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamImportedKeys")
    emptyResponseForSchema(Schemas.GET_IMPORTED_KEYS_SCHEMA, listener)
  }

  override def getStreamCrossReference(command: FlightSql.CommandGetCrossReference,
                                       context: FlightProducer.CallContext,
                                       listener: FlightProducer.ServerStreamListener): Unit = {
    logger.info(s"${clusterManager.getNodeInfo.publicLocation.getUri}: getStreamCrossReference")
    emptyResponseForSchema(Schemas.GET_CROSS_REFERENCE_SCHEMA, listener)
  }

  override def close(): Unit = {
    spark.stop()
    clusterManager.close()
  }

  override def listFlights(context: FlightProducer.CallContext,
                           criteria: Criteria,
                           listener: FlightProducer.StreamListener[FlightInfo]): Unit = {}

  override def getFlightInfoTypeInfo(request: FlightSql.CommandGetXdbcTypeInfo, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def getStreamTypeInfo(request: FlightSql.CommandGetXdbcTypeInfo, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = ???
}
