package org.apache.spark.sql.util

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object ArrowUtilsExtended {

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId)
  }

  def convertToArrowBatchRdd(df: DataFrame): RDD[Array[Byte]] = {
    df.toArrowBatchRdd
  }

}
