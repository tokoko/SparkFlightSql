package com.tokoko.spark.flight.manager

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import java.util.concurrent.ConcurrentLinkedQueue

class Statement {

  val batches = new ConcurrentLinkedQueue[ArrowRecordBatch]();
  var root: VectorSchemaRoot = _

  def setRoot(root: VectorSchemaRoot): Unit = {
    this.root = root
  }

  def getRoot: VectorSchemaRoot = root

  def addBatch(batch: ArrowRecordBatch): Unit = batches.add(batch)

  def nextBatch(): ArrowRecordBatch = batches.poll()

}
