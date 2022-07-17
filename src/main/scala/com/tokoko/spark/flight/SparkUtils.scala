//package com.tokoko.spark.flight
//
//import com.google.protobuf.{Any, ByteString}
//import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, Location}
//import org.apache.arrow.memory.RootAllocator
//import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
//import org.apache.arrow.vector.ipc.ReadChannel
//import org.apache.arrow.vector.ipc.message.MessageSerializer
//import org.apache.arrow.vector.types.pojo.Schema
//import org.apache.spark.rdd.RDD
//import java.io.{ByteArrayInputStream, IOException}
//import java.nio.channels.Channels
//import scala.util.Random
//
//object SparkUtils {
//
//  def applyForEach(rdd: RDD[Array[Byte]],
//                   jsonSchema: String,
//                   handle: ByteString,
//                   serverLocations: List[Location]): Unit = {
//    val serverURIs = serverLocations.map(_.getUri)
//    val handleString = handle.toStringUtf8
//
//    rdd.foreachPartition(it => {
//        val serverLocations = serverURIs.map(uri =>
//          Location.forGrpcInsecure(uri.getHost, uri.getPort)
//        )
//
//        val rootAllocator = new RootAllocator(Long.MaxValue)
//
//        val clients = serverLocations.map(location => {
//          FlightClient.builder(rootAllocator, location).build()
//        })
//
//        val schema = Schema.fromJSON(jsonSchema)
//        val root = VectorSchemaRoot.create(schema, rootAllocator)
//        val descriptor = FlightDescriptor.path(handleString)
//
//        it.foreach(r => {
//            try {
//                val arb = MessageSerializer.deserializeRecordBatch(
//                        new ReadChannel(Channels.newChannel(
//                                new ByteArrayInputStream(r)
//                        )), rootAllocator)
//                val vectorLoader = new VectorLoader(root)
//                vectorLoader.load(arb)
//            } catch {
//              case e: IOException => e.printStackTrace()
//            }
//
//          val clientListener = clients(new Random().nextInt(clients.size))
//            .startPut(descriptor, root, new AsyncPutListener())
//          clientListener.putNext()
//          clientListener.completed();
//        });
//    })
//
//  }
//
//}
