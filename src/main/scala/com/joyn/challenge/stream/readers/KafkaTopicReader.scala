package com.joyn.challenge.stream.readers

import com.joyn.challenge.stream.config.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader

case class KafkaTopicReader(spark: SparkSession, params: Params) extends BaseReader(spark, params) {
  override def reader(): DataStreamReader = {
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", params.kafkaBrokers.head)
        .option("subscribe", params.topics.get.head)
  }
}
