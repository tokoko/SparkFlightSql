package com.tokoko.spark.flight

import com.google.protobuf.Any.pack
import com.google.protobuf.ByteString.copyFrom
import com.google.protobuf.{ByteString, Message}
import com.tokoko.spark.flight.manager.ClusterManager
import org.apache.arrow.flight.{Action, AsyncPutListener, CallStatus, Criteria, FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo, FlightProducer, FlightStream, Location, PutResult, Result, SchemaResult, Ticket}
import org.apache.arrow.flight.sql.{FlightSqlProducer, SqlInfoBuilder}
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.ArrowUtilsExtended

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util
import java.util.UUID.randomUUID
import collection.JavaConverters._
import scala.util.Random

/*_*/
class SparkFlightSqlProducer(val clusterManager: ClusterManager, val spark: SparkSession) extends FlightSqlProducer {
  /*_*/
  private val logger = Logger.getLogger(this.getClass)

  val rootAllocator = new RootAllocator()

  private val localFlightBuffer =  new util.HashMap[ByteString, Statement]

  private val sqlInfoBuilder = new SqlInfoBuilder()
    .withFlightSqlServerName("SparkFlightSql")
    .withFlightSqlServerVersion("3.2.1")
    .withFlightSqlServerArrowVersion("7.0.0")
    .withFlightSqlServerReadOnly(true)

  private def getFlightInfoForSchema(schema: Schema, request: Message, descriptor: FlightDescriptor): FlightInfo = {
    val location = clusterManager.getInfo.publicLocation
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

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    logger.warn("Accept Put Called")
    val path = flightStream.getDescriptor.getPath
    if (path != null && !path.isEmpty) () => {
      def foo(): Unit = {
        val handle = ByteString.copyFromUtf8(path.get(0))

        // TODO
        if (!localFlightBuffer.containsKey(handle)) {
          localFlightBuffer.put(handle, new Statement)
        }

        val statement = localFlightBuffer.get(handle)
        statement.setRoot(flightStream.getRoot)
        while ( {
          flightStream.next
        }) {
          val root = flightStream.getRoot
          new VectorUnloader(root)
          val vectorUnloader = new VectorUnloader(root)
          statement.addBatch(vectorUnloader.getRecordBatch)
        }
        ackStream.onCompleted()
      }

      foo()
    }
    else super.acceptPut(context, flightStream, ackStream)
  }

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
//    logger.warn("DoAction called at " + internalLocation.toString + " " + action.getType)
    if (!clusterManager.handleDoAction(context, action, listener)) {
      super.doAction(context, action, listener)
    }
  }


  /*_*/
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
    logger.warn("GetFlightInfo called at " + "internalLocation.toString")
    val handle = copyFrom(randomUUID.toString.getBytes(StandardCharsets.UTF_8))
    val sparkSchema = spark.sql(command.getQuery).schema
    val arrowSchema = ArrowUtilsExtended.toArrowSchema(sparkSchema, spark.sessionState.conf.sessionLocalTimeZone)

    val ticketStatementQuery = FlightSql.TicketStatementQuery.newBuilder.setStatementHandle(handle).build

    val ticket = new Ticket(pack(ticketStatementQuery).toByteArray)
    val query = command.getQuery
    clusterManager.addFlight(handle)

    new Thread(() => {
      val df = spark.sql(query).repartition(3)
      val abr = ArrowUtilsExtended.convertToArrowBatchRdd(df)
      val jsonSchema = arrowSchema.toJson
      val handleString = handle.toStringUtf8
      val serverURIs = clusterManager.getNodes.map(_.internalLocation).map(_.getUri)

      abr.foreachPartition(it => {
        val serverLocations = serverURIs.map(uri =>
          Location.forGrpcInsecure(uri.getHost, uri.getPort)
        )

        val rootAllocator = new RootAllocator(Long.MaxValue)

        val clients = serverLocations.map(location => {
          FlightClient.builder(rootAllocator, location).build()
        })

        clients.foreach(c => c.authenticateBasic("user", "password"))

        val schema = Schema.fromJSON(jsonSchema)
        val root = VectorSchemaRoot.create(schema, rootAllocator)
        val descriptor = FlightDescriptor.path(handleString)

        it.foreach(r => {
          try {
            val arb = MessageSerializer.deserializeRecordBatch(
              new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(r)
              )), rootAllocator)
            val vectorLoader = new VectorLoader(root)
            vectorLoader.load(arb)
          } catch {
            case e: IOException => e.printStackTrace()
          }

          val clientListener = clients(new Random().nextInt(clients.size))
            .startPut(descriptor, root, new AsyncPutListener())
          clientListener.putNext()
          clientListener.completed();
        });
      })

      clusterManager.setCompleted(handle)
    }).start()

    val endpoints = clusterManager.getNodes.map(_.publicLocation)
      .map(serverLocation => new FlightEndpoint(ticket, serverLocation))

    new FlightInfo(arrowSchema, descriptor, endpoints.asJava, -1, -1)
  }

  override def getFlightInfoPreparedStatement(command: FlightSql.CommandPreparedStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
  }

  override def getSchemaStatement(command: FlightSql.CommandStatementQuery, context: FlightProducer.CallContext, descriptor: FlightDescriptor): SchemaResult = ???

  override def getStreamStatement(ticket: FlightSql.TicketStatementQuery, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    //logger.warn("GetStream called at " + internalLocation.toString)
    val handle = ticket.getStatementHandle

    // TODO
    if (!localFlightBuffer.containsKey(handle)) {
      localFlightBuffer.put(handle, new Statement)
    }

    val statement = localFlightBuffer.get(handle)

    if (statement == null) {
//      logger.warn("Couldn't locate requested statement")
      listener.error(new Exception("Couldn't locate requested statement"))
      return
    }

    while ( {
      statement.getRoot == null && !(clusterManager.getStatus(handle) == "COMPLETED")
    }) try {
//      logger.warn("Waiting for statement VectorSchemaRoot: Sleeping for 1 second")
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException =>
        throw new RuntimeException(e)
    }

    val root = statement.getRoot

    var completed = false

    if (root != null) {
      listener.start(root)

      while (!completed) {
        val batch = statement.nextBatch()
        val loader = new VectorLoader(root)
        if (batch == null) {
          if (clusterManager.getStatus(handle) == "COMPLETED") {
            completed = true
          } else {
            try {
              //            logger.warn("Waiting for additional ArrowRecordBatches: Sleeping for 1 second")
              Thread.sleep(1000)
            } catch {
              case e: InterruptedException => throw new RuntimeException(e)
            }
          }
        } else {
          if (!completed) {
            try loader.load(batch)
            catch {
              case ex: Exception =>
                ex.printStackTrace()
                throw ex
            }
            listener.putNext()
          }
        }
      }
    }

    listener.completed()
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
    getFlightInfoForSchema(Schemas.GET_SQL_INFO_SCHEMA, request, descriptor)
  }

  override def getStreamSqlInfo(command: FlightSql.CommandGetSqlInfo, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
    sqlInfoBuilder.send(command.getInfoList, listener)
  }

  override def getFlightInfoCatalogs(request: FlightSql.CommandGetCatalogs,
                                     context: FlightProducer.CallContext,
                                     descriptor: FlightDescriptor): FlightInfo = {
    getFlightInfoForSchema(Schemas.GET_CATALOGS_SCHEMA, request, descriptor)
  }

  override def getStreamCatalogs(context: FlightProducer.CallContext,
                                 listener: FlightProducer.ServerStreamListener): Unit = {
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
    getFlightInfoForSchema(Schemas.GET_SCHEMAS_SCHEMA, request, descriptor)
  }

  override def getStreamSchemas(command: FlightSql.CommandGetDbSchemas,
                                context: FlightProducer.CallContext,
                                listener: FlightProducer.ServerStreamListener): Unit = {
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
    val schema = if (request.getIncludeSchema) Schemas.GET_TABLES_SCHEMA else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
    getFlightInfoForSchema(schema, request, descriptor)
  }

  override def getStreamTables(command: FlightSql.CommandGetTables, context: FlightProducer.CallContext, listener: FlightProducer.ServerStreamListener): Unit = {
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
    if (!CatalogUtils.tableExists(spark, request.getCatalog, request.getDbSchema, request.getTable))
      throw CallStatus.NOT_FOUND.toRuntimeException

    getFlightInfoForSchema(Schemas.GET_PRIMARY_KEYS_SCHEMA, request, descriptor)
  }

  override def getFlightInfoExportedKeys(request: FlightSql.CommandGetExportedKeys,
                                         context: FlightProducer.CallContext,
                                         descriptor: FlightDescriptor): FlightInfo = {
    if (!CatalogUtils.tableExists(spark, request.getCatalog, request.getDbSchema, request.getTable))
      throw CallStatus.NOT_FOUND.toRuntimeException

    getFlightInfoForSchema(Schemas.GET_EXPORTED_KEYS_SCHEMA, request, descriptor)
  }

  override def getFlightInfoImportedKeys(request: FlightSql.CommandGetImportedKeys,
                                         context: FlightProducer.CallContext,
                                         descriptor: FlightDescriptor): FlightInfo = {
    if (!CatalogUtils.tableExists(spark, request.getCatalog, request.getDbSchema, request.getTable))
      throw CallStatus.NOT_FOUND.toRuntimeException

    getFlightInfoForSchema(Schemas.GET_IMPORTED_KEYS_SCHEMA, request, descriptor)
  }

  override def getFlightInfoCrossReference(request: FlightSql.CommandGetCrossReference,
                                           context: FlightProducer.CallContext,
                                           descriptor: FlightDescriptor): FlightInfo = {
    if (
      !CatalogUtils.tableExists(spark, request.getPkCatalog, request.getPkDbSchema, request.getPkTable) ||
        !CatalogUtils.tableExists(spark, request.getFkCatalog, request.getFkDbSchema, request.getFkTable)
    ) throw CallStatus.NOT_FOUND.toRuntimeException

    getFlightInfoForSchema(Schemas.GET_CROSS_REFERENCE_SCHEMA, request, descriptor)
  }

  override def getStreamPrimaryKeys(command: FlightSql.CommandGetPrimaryKeys,
                                    context: FlightProducer.CallContext,
                                    listener: FlightProducer.ServerStreamListener): Unit = {
    emptyResponseForSchema(Schemas.GET_PRIMARY_KEYS_SCHEMA, listener)
  }

  override def getStreamExportedKeys(command: FlightSql.CommandGetExportedKeys,
                                     context: FlightProducer.CallContext,
                                     listener: FlightProducer.ServerStreamListener): Unit = {
    emptyResponseForSchema(Schemas.GET_EXPORTED_KEYS_SCHEMA, listener)
  }

  override def getStreamImportedKeys(command: FlightSql.CommandGetImportedKeys,
                                     context: FlightProducer.CallContext,
                                     listener: FlightProducer.ServerStreamListener): Unit = {
    emptyResponseForSchema(Schemas.GET_IMPORTED_KEYS_SCHEMA, listener)
  }

  override def getStreamCrossReference(command: FlightSql.CommandGetCrossReference,
                                       context: FlightProducer.CallContext,
                                       listener: FlightProducer.ServerStreamListener): Unit = {
    emptyResponseForSchema(Schemas.GET_CROSS_REFERENCE_SCHEMA, listener)
  }

  override def close(): Unit = {
    spark.stop()
    clusterManager.close()
  }

  override def listFlights(context: FlightProducer.CallContext,
                           criteria: Criteria,
                           listener: FlightProducer.StreamListener[FlightInfo]): Unit = {

  }

}
/*_*/
