package org.apache.spark.sql.util

import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types.StructType

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Collections
import collection.JavaConverters._

object ArrowUtilsExtended {

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(schema)
  }

  def convertToArrowBatchRdd(df: DataFrame): RDD[Array[Byte]] = {
    df.toArrowBatchRdd
  }

  def fromBatchIterator(arrowBatchIter: Iterator[Array[Byte]],
                        schema: StructType,
                        timeZoneId: String,
                        context: TaskContext): Iterator[InternalRow] = {
    ArrowConverters.fromBatchIterator(arrowBatchIter, schema, timeZoneId, context)
  }

  def fromVectorSchemaRoot(root: VectorSchemaRoot): Iterator[InternalRow] = {
    val sparkSchema = ArrowUtils.fromArrowSchema(root.getSchema)
    val unloader = new VectorUnloader(root)
    val arb = unloader.getRecordBatch
    val out = new ByteArrayOutputStream
    val channel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(channel, arb)

    val iter = Collections.singletonList(out.toByteArray)

    fromBatchIterator(
      iter.iterator().asScala,
      sparkSchema,
      "UTC",
      //spark.sqlContext.sessionState.conf.sessionLocalTimeZone,
      TaskContext.get()
    )
  }

}
