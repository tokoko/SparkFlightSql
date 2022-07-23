package com.tokoko.spark.flight.manager

import com.google.protobuf.ByteString
import org.apache.arrow.flight.{Action, ActionType, CallStatus, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}
import org.apache.arrow.vector.VectorUnloader
import scala.collection.mutable

class ManagerFlightProducer(localFlightBuffer: mutable.Map[ByteString, Statement]) extends FlightProducer {

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.toRuntimeException

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit =
    throw CallStatus.UNIMPLEMENTED.toRuntimeException

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.toRuntimeException

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    () => {
      flightStream.getDescriptor.getPath
      val path = flightStream.getDescriptor.getPath
      val handle = ByteString.copyFromUtf8(path.get(0))

      // TODO
      if (!localFlightBuffer.contains(handle)) {
        localFlightBuffer.put(handle, new Statement)
      }

      val statement = localFlightBuffer(handle)

      statement.setRoot(flightStream.getRoot)
      while (flightStream.next) {
        val root = flightStream.getRoot
        new VectorUnloader(root)
        val vectorUnloader = new VectorUnloader(root)
        statement.addBatch(vectorUnloader.getRecordBatch)
      }
      ackStream.onCompleted()
    }
  }

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit =
    throw CallStatus.UNIMPLEMENTED.toRuntimeException

  override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit =
    throw CallStatus.UNIMPLEMENTED.toRuntimeException
}
