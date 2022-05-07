package org.apache.spark.sql.util

import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.arrow.ArrowConverters

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Collections

object ArrowHelpers {

  def toDataFrame(spark: SparkSession, root: VectorSchemaRoot): DataFrame = {
    val sparkSchema = ArrowUtils.fromArrowSchema(root.getSchema)
    val unloader = new VectorUnloader(root)
    val arb = unloader.getRecordBatch
    val out = new ByteArrayOutputStream
    val channel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(channel, arb)
    val rdd = new JavaSparkContext(spark.sparkContext).parallelize(Collections.singletonList(out.toByteArray))
    ArrowConverters.toDataFrame(rdd, sparkSchema.json, spark.sqlContext)
  }

}
