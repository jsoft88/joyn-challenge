package com.joyn.challenge.stream.jobs.kafka

import com.joyn.challenge.stream.config.{Params, ParamsBuilder}
import com.joyn.challenge.stream.core.StreamJob
import com.joyn.challenge.stream.factory.{ReaderFactory, TransformationFactory, WriterFactory}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait SchemaType {
  def getDefaultWatermarkColName(): String
}

/**
 * Consider this companion object to be a sort of Schema registry.
 * even though the whole idea is having a flexible framework open for extensibility,
 * e.g. take topics names as CLI parameters instead of hardcoding it, it is still
 * valid to map {@code SchemaType} child classes {@code toString} method to the different
 * topics the organisation has, because this is our schema registry.
 */
object PageViewsStream {
  val DefaultDelaySeconds: Long = 0L
  val DefaultWindowDuration: Long = 0L
  val DefaultSlidingWindowInterval: Long = 0L
  val DefaultUsersWatermarkField = "timestamp"
  val DefaultPageViewsWatermarkField = "timestamp"

  case object UsersSchema extends SchemaType {
    override def toString: String = "users"

    override def getDefaultWatermarkColName(): String = DefaultUsersWatermarkField
  }

  case object PageViewsSchema extends SchemaType {
    override def toString: String = "pageviews"

    override def getDefaultWatermarkColName(): String = DefaultPageViewsWatermarkField
  }

  private val pageViewsSchema = new StructType()
    .add("viewtime", LongType)
    .add("userid", StringType)
    .add("pageid", StringType)

  private val usersSchema = new StructType()
    .add("userid", StringType)
    .add("gender", StringType)
    .add("regionid", StringType)
    .add("registertime", LongType)

  val SchemaByType: Map[SchemaType, StructType] = Map(
    UsersSchema -> usersSchema,
    PageViewsSchema -> pageViewsSchema
  )

  val AllSchemaTypes: Seq[SchemaType] = Seq(
    PageViewsStream.PageViewsSchema,
    PageViewsStream.UsersSchema
  )
}

class PageViewsStream(params: Params) extends StreamJob[Params](params){
  var spark: SparkSession = _
  var usersDelaySeconds: Long = 0L
  var pageViewsDelaySeconds: Long = 0L
  var topicsDelayPair: Map[String, Long] = Map.empty
  var writeInterval: Long = 0L
  var kafkaBrokers: String = _
  var topicEventField: Option[Map[String, String]] = None
  var schemaByTopic: Map[String, StructType] = Map.empty
  var topics: Seq[String] = Seq.empty

  private def getSchemaByType(schemaType: String): StructType = {
    PageViewsStream.AllSchemaTypes.filter(st => st.toString.toLowerCase.equals(schemaType)) headOption match {
      case None => throw new IllegalArgumentException(s"Invalid schema type ${schemaType} provided")
      case Some(st) => PageViewsStream.SchemaByType.get(st).get
    }
  }

  override protected def setupJob(): Unit = {
    params.delayPerTopic match {
      case None => throw new IllegalArgumentException("Streaming from kafka requires topics to be present, but None found")
      case Some(topics) => this.topicsDelayPair = topics
    }

    this.writeInterval = params.writeInterval
    params.kafkaBrokers match {
      case None => throw new IllegalArgumentException("Streaming from kafka requires bootstrap servers, but None found")
      case Some(kb) => this.kafkaBrokers = kb
    }

    params.eventTimeFieldPerTopic match {
      case None => {
        // When None, throw an exception as the consumer is responsible for defining the name of a column to be used for
        // watermark, even when the stream does not have such column, still the user is responsible for giving a name for
        // watermark columns for each topic.
        throw new IllegalArgumentException("Expected name for watermark columns but None found")

      }
      case Some(evt) => (evt.get(PageViewsStream.UsersSchema.toString), evt.get(PageViewsStream.PageViewsSchema.toString)) match {
        case (Some(_), Some(_)) => this.topicEventField = Some(evt)
        case (None, _) => throw new IllegalArgumentException("Expected name for watermark columns but None found")
        case (_, None) => throw new IllegalArgumentException("Expected name for watermark columns but None found")
      }
    }

    params.schemaTypeByTopic match {
      case None => throw new IllegalArgumentException("Expected schema types of topics to be defined, but None found")
      case Some(stbt) => this.schemaByTopic ++= stbt.map(kv => kv._1 -> this.getSchemaByType(kv._2))
    }

    params.topics match {
      case None => throw new IllegalArgumentException("Expected topics to be present, but None found")
      case Some(t) => this.topics = t
    }
  }

  override protected def setupInputStream(): Option[Map[String, DataFrame]] = {
    // Iterate the different topics we obtained via CLI to load each dataframe.
    Some(this.topics.map(t => {
      val schemaType = PageViewsStream
        .AllSchemaTypes
        .filter(_.toString.equals(t))
        .head
      val watermarkCol: String = PageViewsStream.SchemaByType.get(schemaType)
        .get.filter(_.name.equals(this.topicEventField.get.get(t).get)).headOption match {
        case None => schemaType.getDefaultWatermarkColName()
        case Some(f) => f.name
      }
      val kafkaParams = new ParamsBuilder().withKafkaBrokers(this.kafkaBrokers).withTopics(Some(Seq(t))).build()

      t -> ReaderFactory
        .getReader(ReaderFactory.KafkaReader, this.spark, kafkaParams)
        .reader()
        .load()
        .select(from_json(col("value").cast(StringType), this.schemaByTopic.get(t).get))
        .withColumn(s"${this.topicEventField.get.get(t).head}", col(s"${watermarkCol}").cast(TimestampType))
        .withWatermark(s"${this.topicEventField.get.get(t).head}", s"${this.topicsDelayPair.get(t).get} seconds")
    }) toMap)
  }

  override protected def transform(dataframes: Option[Map[String, DataFrame]]): DataFrame = {
    TransformationFactory.getTransformation(TransformationFactory.Top10ByGender, this.spark, params)
      .transformStream(dataframes) match {
      case None => throw new Exception("Error occurred while transforming stream, expected dataframe but None was found.")
      case Some(df) => df
    }
  }

  override protected def writeStream(dataFrame: Option[DataFrame]): Unit = {
    WriterFactory.getWriter(WriterFactory.KafkaWriter, this.spark, params).writer(
      dataFrame,
      TransformationFactory.getTransformation(TransformationFactory.NoOp, spark, params)
    ).start()
  }

  override protected def finalizeJob(): Unit = {
    this.spark.stop()
  }
}
