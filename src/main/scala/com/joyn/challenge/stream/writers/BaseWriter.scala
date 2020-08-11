package com.joyn.challenge.stream.writers

import com.joyn.challenge.stream.config.Params
import com.joyn.challenge.stream.transformation.BaseTransform
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

abstract class BaseWriter(spark: SparkSession, params: Params) {
  def writer(dataframe: Option[DataFrame], transformInstance: BaseTransform): DataStreamWriter[Row]
}
