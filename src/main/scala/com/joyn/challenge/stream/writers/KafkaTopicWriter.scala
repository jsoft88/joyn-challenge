package com.joyn.challenge.stream.writers

import java.util.concurrent.TimeUnit

import com.joyn.challenge.stream.config.Params
import com.joyn.challenge.stream.transformation.BaseTransform
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

case class KafkaTopicWriter(spark: SparkSession, params: Params) extends BaseWriter(spark, params) {
  override def writer(dataframe: Option[DataFrame], transformInstance: BaseTransform): DataStreamWriter[Row] = {
    dataframe match {
      case None => throw new IllegalArgumentException("Cannot obtain writer for dataframe None")
      case Some(df) => {
        df
          .writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", params.kafkaBrokers.get)
          .option("topic", params.outputTopic.get)
          .outputMode(OutputMode.Append)
          .trigger(Trigger.ProcessingTime(params.writeInterval, TimeUnit.SECONDS))
      }
    }
  }
}
