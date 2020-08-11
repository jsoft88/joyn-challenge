package com.joyn.challenge.stream.factory

import com.joyn.challenge.stream.config.Params
import com.joyn.challenge.stream.readers.{BaseReader, KafkaTopicReader}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrameReader, SparkSession}

sealed trait ReaderType

object ReaderFactory {
  case object KafkaReader extends ReaderType {
    override def toString: String = "kafka"
  }

  val AllReaderTypes = Seq(
    KafkaReader
  )

  def getReader(readerType: ReaderType, spark: SparkSession, params: Params): BaseReader = {
    readerType match {
      case KafkaReader => KafkaTopicReader(spark, params)
      case _ => throw new IllegalArgumentException(s"Invalid type for reader factory provided")
    }
  }

  def getReaderType(readerType: String): ReaderType = {
    AllReaderTypes.filter(_.toString.toLowerCase.equals(readerType)) headOption match {
      case None => throw new IllegalArgumentException(s"Invalid reader type: ${readerType} passed.")
      case Some(t) => t
    }
  }
}
