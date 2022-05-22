package com.tokoko.spark.flight;

import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.google.protobuf.Message;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import scala.Tuple2;
import scala.Tuple3;
import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;


public class SparkFlightSqlProducer implements FlightSqlProducer {

    private final Logger logger = Logger.getLogger(this.getClass());

    private final Location internalLocation;
    private final Location publicLocation;
    private final SparkSession spark;
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final FlightManager flightManager;
    private final HashMap<ByteString, Statement> localFlightBuffer;
    private final List<Location> internalPeerLocations;
    private final List<Location> publicPeerLocations;

    private final SqlInfoBuilder sqlInfoBuilder;

    public SparkFlightSqlProducer(final Location internalLocation,
                                  final Location publicLocation,
                                  final SparkSession spark) {
        this(internalLocation, publicLocation, spark, new Tuple2[] {});
    }

    public SparkFlightSqlProducer(final Location internalLocation,
                                  final Location publicLocation,
                                  final SparkSession spark,
                                  Tuple2<Location, Location>[] peerLocations
    ) {
        this.internalLocation = internalLocation;
        this.publicLocation = publicLocation;
        this.spark = spark;

        this.internalPeerLocations = Arrays.stream(peerLocations).map(peer -> peer._1)
                .collect(Collectors.toList());

        internalPeerLocations.add(internalLocation);

        this.publicPeerLocations = Arrays.stream(peerLocations).map(peer -> peer._2)
                .collect(Collectors.toList());

        publicPeerLocations.add(publicLocation);

        this.flightManager = new FlightManager(
                internalPeerLocations.stream().filter(peer -> peer != internalLocation)
                        .collect(Collectors.toList())
                , rootAllocator);

        this.localFlightBuffer = new HashMap<>();

        this.sqlInfoBuilder = new SqlInfoBuilder()
                .withFlightSqlServerName("SparkFlightSql")
                .withFlightSqlServerVersion("3.2.1")
                .withFlightSqlServerArrowVersion("7.0.0")
                .withFlightSqlServerReadOnly(true);
    }

    private FlightInfo getFlightInfoForSchema(Schema schema, Message request, FlightDescriptor descriptor) {
        final Ticket ticket = new Ticket(pack(request).toByteArray());
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, publicLocation));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

//    private FlightInfo getFlightInfoForSchemaQuery(Schema schema,
//                                     FlightDescriptor descriptor,
//                                     String query) {
//        ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
//
//        FlightSql.TicketStatementQuery ticketStatementQuery = FlightSql.TicketStatementQuery.newBuilder()
//                .setStatementHandle(handle)
//                .build();
//
//        final Ticket ticket = new Ticket(pack(ticketStatementQuery).toByteArray());
//
//        localFlightBuffer.put(handle, new Statement());
//
//        flightManager.addFlight(handle);
////        flightManager.broadcast(handle);
//
//        new Thread(() -> {
//            Dataset<Row> df = spark.sql(query);
//            RDD<byte[]> abr = df.toArrowBatchRdd();
//
//            scala.collection.Iterator<byte[]> it = abr.toLocalIterator();
//
//            Statement statement = localFlightBuffer.get(handle);
//
//            VectorSchemaRoot root = VectorSchemaRoot.create(schema, rootAllocator);
//
//            statement.setRoot(root);
//
//            it.foreach(r -> {
//                try {
//                    ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(
//                            new ReadChannel(Channels.newChannel(
//                                    new ByteArrayInputStream(r)
//                            )), rootAllocator);
//
//                    statement.addBatch(arb);
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                return null;
//            });
//                flightManager.setCompleted(handle);
////                flightManager.broadcast(handle);
//        }).start();
//
////        final List<FlightEndpoint> endpoints = publicPeerLocations
////                .stream().map(serverLocation -> new FlightEndpoint(ticket, serverLocation))
////                .collect(Collectors.toList());
//
//        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, publicLocation));
//
//        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
//    }
//
//    private void getStreamResponse(FlightSql.TicketStatementQuery ticket, ServerStreamListener listener) {
//        ByteString handle = ticket.getStatementHandle();
//
//        Statement statement = localFlightBuffer.get(handle);
//
//        if (statement == null) {
//            logger.warn("Couldn't locate requested statement");
//            listener.error(new Exception("Couldn't locate requested statement"));
//            return;
//        }
//
//        while (statement.getRoot() == null && !flightManager.getStatus(handle).equals("COMPLETED")) {
//            try {
//                logger.warn("Waiting for statement VectorSchemaRoot: Sleeping for 1 second");
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        VectorSchemaRoot root = statement.getRoot();
//
//        if (root != null) {
//            listener.start(root);
//
//            while (true) {
//                ArrowRecordBatch batch = statement.nextBatch();
//                VectorLoader loader = new VectorLoader(root);
//                if (batch == null) {
//                    if (flightManager.getStatus(handle).equals("COMPLETED")) {
//                        break;
//                    } else {
//                        try {
//                            logger.warn("Waiting for additional ArrowRecordBatches: Sleeping for 1 second");
//                            Thread.sleep(1000);
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        }
//                        continue;
//                    }
//                }
//
//                try {
//                    loader.load(batch);
//                } catch (Exception ex) {
//                    ex.printStackTrace();
//                    throw ex;
//                }
//
//                listener.putNext();
//            }
//        }
//
//        listener.completed();
//    }

    private void emptyResponseForSchema(Schema schema, ServerStreamListener listener) {
        VectorSchemaRoot table = VectorSchemaRoot.create(schema, rootAllocator);
        table.setRowCount(0);
        listener.start(table);
        listener.putNext();
        listener.completed();
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    // TODO
    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        logger.warn("GetFlightInfo called at " + internalLocation.toString());
        ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        StructType sparkSchema = spark.sql(command.getQuery()).schema();
        Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, spark.sessionState().conf().sessionLocalTimeZone());

        FlightSql.TicketStatementQuery ticketStatementQuery = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle)
                .build();

        final Ticket ticket = new Ticket(pack(ticketStatementQuery).toByteArray());
        String query = command.getQuery();

        localFlightBuffer.put(handle, new Statement());

        flightManager.addFlight(handle);
        flightManager.broadcast(handle);

        if (true) {
            new Thread(() -> {
                Dataset<Row> df = spark.sql(query).repartition(3);
                RDD<byte[]> abr = df.toArrowBatchRdd();
                SparkUtils.applyForEach(abr, arrowSchema.toJson(), handle, internalPeerLocations);
                flightManager.setCompleted(handle);
                flightManager.broadcast(handle);
            }).start();
        } else {

            new Thread(() -> {
                Dataset<Row> df = spark.sql(query).repartition(3);
                RDD<byte[]> abr = df.toArrowBatchRdd();

                scala.collection.Iterator<byte[]> it = abr.toLocalIterator();

                Statement statement = localFlightBuffer.get(handle);

                VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, rootAllocator);

                statement.setRoot(root);

                it.foreach(r -> {
                    try {
                        ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(
                                new ReadChannel(Channels.newChannel(
                                        new ByteArrayInputStream(r)
                                )), rootAllocator);

                        statement.addBatch(arb);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                });
                flightManager.setCompleted(handle);
                flightManager.broadcast(handle);
            }).start();

        }
        final List<FlightEndpoint> endpoints = publicPeerLocations
                .stream().map(serverLocation -> new FlightEndpoint(ticket, serverLocation))
                .collect(Collectors.toList());

//        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, location));
        return new FlightInfo(arrowSchema, descriptor, endpoints, -1, -1);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
        logger.warn("GetStream called at " + internalLocation.toString());
        ByteString handle = ticket.getStatementHandle();

        Statement statement = localFlightBuffer.get(handle);

        if (statement == null) {
            logger.warn("Couldn't locate requested statement");
            listener.error(new Exception("Couldn't locate requested statement"));
            return;
        }

        while (statement.getRoot() == null && !flightManager.getStatus(handle).equals("COMPLETED")) {
            try {
                logger.warn("Waiting for statement VectorSchemaRoot: Sleeping for 1 second");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        VectorSchemaRoot root = statement.getRoot();

        if (root != null) {

            listener.start(root);

            while (true) {
                ArrowRecordBatch batch = statement.nextBatch();
                VectorLoader loader = new VectorLoader(root);
                if (batch == null) {
                    if (flightManager.getStatus(handle).equals("COMPLETED")) {
                        break;
                    } else {
                        try {
                            logger.warn("Waiting for additional ArrowRecordBatches: Sleeping for 1 second");
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        continue;
                    }
                }

                try {
                    loader.load(batch);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }

                listener.putNext();
            }
        }

        listener.completed();
    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(Schemas.GET_SQL_INFO_SCHEMA, request, descriptor);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
        this.sqlInfoBuilder.send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(Schemas.GET_CATALOGS_SCHEMA, request, descriptor);
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        VectorSchemaRoot table = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, rootAllocator);
        List<String> catalogs = CatalogUtils.listCatalogs(spark);
        VarCharVector nameVector = (VarCharVector) table.getVector(0);
        nameVector.allocateNew(catalogs.size());
        for(int i = 0; i < catalogs.size(); i++) {
            nameVector.set(i, catalogs.get(i).getBytes());
        }
        nameVector.setValueCount(catalogs.size());
        table.setRowCount(catalogs.size());
        listener.start(table);
        listener.putNext();
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(Schemas.GET_SCHEMAS_SCHEMA, request, descriptor);
    }

    // TODO
    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        String catalog = command.getCatalog();
        String filterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;

        VectorSchemaRoot table = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, rootAllocator);

        List<Tuple2<String, String>> schemas = CatalogUtils.listNamespaces(spark, catalog, filterPattern);

        VarCharVector catalogVector = (VarCharVector) table.getVector(0);
        VarCharVector schemaVector = (VarCharVector) table.getVector(1);

        catalogVector.allocateNew(schemas.size());
        schemaVector.allocateNew(schemas.size());

        for(int i = 0; i < schemas.size(); i++) {
            catalogVector.set(i, schemas.get(i)._1.getBytes());
            schemaVector.set(i, schemas.get(i)._2.getBytes());
        }

        catalogVector.setValueCount(schemas.size());
        schemaVector.setValueCount(schemas.size());
        table.setRowCount(schemas.size());

        listener.start(table);
        listener.putNext();
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context, FlightDescriptor descriptor) {
        Schema schema = request.getIncludeSchema() ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        return getFlightInfoForSchema(schema, request, descriptor);
    }

    // TODO
    @Override
    public void getStreamTables(FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
        Schema schema = command.getIncludeSchema() ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;

        String catalog = command.getCatalog();
        String schemaPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        String tablePattern = command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;
//        command.getTableTypesList().asByteStringList().
        boolean includeSchema = command.getIncludeSchema();

        List<Tuple3<String, String, String>> tables =
                CatalogUtils.listTables(spark, catalog, schemaPattern, tablePattern);

        VectorSchemaRoot res = VectorSchemaRoot.create(schema, rootAllocator);

        List<ArrowType> fields = schema.getFields().stream().map(Field::getType)
                .collect(Collectors.toList());

        for (int j = 0; j < fields.size(); j++) {
            res.getVector(j).allocateNew();
        }

        for (int i = 0; i < tables.size(); i++) {
            ((VarCharVector) res.getVector("table_name")).set(i, tables.get(i)._3().getBytes());
            ((VarCharVector) res.getVector("db_schema_name")).set(i, tables.get(i)._2().getBytes());
            ((VarCharVector) res.getVector("catalog_name")).set(i, tables.get(i)._1().getBytes());
            ((VarCharVector) res.getVector("table_type")).set(i, "MANAGED".getBytes());
            if (includeSchema) {
                StructType sparkSchema = spark.table(tables.get(i)._1() + "." + tables.get(i)._2() + "." + tables.get(i)._3()).schema();
                Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, spark.sessionState().conf().sessionLocalTimeZone());

                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

                try {
                    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), arrowSchema);
                } catch (IOException e) {}

                ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());

                ((VarBinaryVector) res.getVector("table_schema"))
                        .set(i, copyFrom(buffer).toByteArray());

            }
        }

        res.setRowCount(tables.size());
        listener.start(res);
        listener.putNext();
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(Schemas.GET_TABLE_TYPES_SCHEMA, request, descriptor);
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        VectorSchemaRoot table = VectorSchemaRoot.create(Schemas.GET_TABLE_TYPES_SCHEMA, rootAllocator);
        VarCharVector tableTypesVector = (VarCharVector) table.getVector(0);
        tableTypesVector.allocateNew(3);
        tableTypesVector.set(0, "EXTERNAL".getBytes());
        tableTypesVector.set(1, "INTERNAL".getBytes());
        tableTypesVector.set(2, "VIEW".getBytes());
        tableTypesVector.setValueCount(3);
        table.setRowCount(3);
        listener.start(table);
        listener.putNext();
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context, FlightDescriptor descriptor) {
        if (!CatalogUtils.tableExists(spark, request.getCatalog(), request.getDbSchema(), request.getTable())) {
            throw CallStatus.NOT_FOUND.toRuntimeException();
        }
        return getFlightInfoForSchema(Schemas.GET_PRIMARY_KEYS_SCHEMA, request, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context, FlightDescriptor descriptor) {
        if (!CatalogUtils.tableExists(spark, request.getCatalog(), request.getDbSchema(), request.getTable())) {
            throw CallStatus.NOT_FOUND.toRuntimeException();
        }
        return getFlightInfoForSchema(Schemas.GET_EXPORTED_KEYS_SCHEMA, request, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context, FlightDescriptor descriptor) {
        if (!CatalogUtils.tableExists(spark, request.getCatalog(), request.getDbSchema(), request.getTable())) {
            throw CallStatus.NOT_FOUND.toRuntimeException();
        }
        return getFlightInfoForSchema(Schemas.GET_IMPORTED_KEYS_SCHEMA, request, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context, FlightDescriptor descriptor) {
        if (
            !CatalogUtils.tableExists(spark, request.getPkCatalog(), request.getPkDbSchema(), request.getPkTable()) ||
            !CatalogUtils.tableExists(spark, request.getFkCatalog(), request.getFkDbSchema(), request.getFkTable())
        ) {
            throw CallStatus.NOT_FOUND.toRuntimeException();
        }

        return getFlightInfoForSchema(Schemas.GET_CROSS_REFERENCE_SCHEMA, request, descriptor);
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {
        emptyResponseForSchema(Schemas.GET_PRIMARY_KEYS_SCHEMA, listener);
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context, ServerStreamListener listener) {
        emptyResponseForSchema(Schemas.GET_EXPORTED_KEYS_SCHEMA, listener);
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {
        emptyResponseForSchema(Schemas.GET_IMPORTED_KEYS_SCHEMA, listener);
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {
        emptyResponseForSchema(Schemas.GET_CROSS_REFERENCE_SCHEMA, listener);
    }

    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        logger.warn("DoAction called at " + internalLocation.toString() + " " + action.getType());

        if (action.getType().equals("RUNNING")) {
            flightManager.addFlight(ByteString.copyFrom(action.getBody()));
            localFlightBuffer.put(ByteString.copyFrom(action.getBody()), new Statement());
            listener.onCompleted();
        } else if (action.getType().equals("COMPLETED")) {
            flightManager.setCompleted(ByteString.copyFrom(action.getBody()));
            listener.onCompleted();
        } else FlightSqlProducer.super.doAction(context, action, listener);

    }

    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        List<String> path = flightStream.getDescriptor().getPath();
        if (path != null && !path.isEmpty()) {
            return () -> {
                ByteString handle = ByteString.copyFromUtf8(path.get(0));
                Statement statement = localFlightBuffer.get(handle);
                statement.setRoot(flightStream.getRoot());

                while (flightStream.next()) {
                    VectorSchemaRoot root = flightStream.getRoot();
                    new VectorUnloader(root);
                    VectorUnloader vectorUnloader = new VectorUnloader(root);
                    statement.addBatch(vectorUnloader.getRecordBatch());
                }
                ackStream.onCompleted();
            };
        } else return FlightSqlProducer.super.acceptPut(context, flightStream, ackStream);
    }

    @Override
    public void close() {
        spark.stop();
    }

    // TODO
    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

    }
}