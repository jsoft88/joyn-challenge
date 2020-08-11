package com.joyn.challenge.stream.readers

import com.joyn.challenge.stream.config.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader

abstract class BaseReader(spark: SparkSession, params: Params) {
  def reader(): DataStreamReader
}
