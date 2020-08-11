package com.joyn.challenge.stream.transformation

import com.joyn.challenge.stream.config.Params
import com.joyn.challenge.stream.jobs.kafka.PageViewsStream
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum, window}
import org.apache.spark.sql.avro._

/**
 * Given that transformations are made based on stakeholders requirements, we will have different
 * classes here for providing such functionalities. In this class, Joyn recruitment team asked the following:
 * ------------------------------------------
 * - Joins the messages in these two topics on the user id field
 * - Uses a 1 minute hopping window with 10 second advances to compute the 10 most viewed pages by viewtime for every value of gender
 * - Once per minute produces a message into the top_pages topic that contains the gender, page id, sum of view time in the latest window and distinct count of user ids in the latest window
 * ------------------------------------------
 *
 * So this is a class providing the transformations required in this request. As more requests arrive to the team, we could
 * extend the framework and provide it via Factory.
 *
 * {@note}: Notice that there's no loss of generalization by directly referencing
 * {@code com.joyn.challenge.stream.jobs.kafka.PageViewsStream.{PageViewsSchema, UsersSchema}, since as mentioned
 * in {@see com.joyn.challenge.stream.jobs.kafka.PageViewsStream}, I am using the companion object as a schema registry.
 *
 * Given that the transformation is specific to a requirement, it is ok to reference directly, even though the user is prompted
 * to provide topics via CLI params. However, this allows for flexibility in other components, say, readers.
 *
 * This is not different than directly accessing column names in the dataframes.
 * @param spark an active spark session
 * @param params parameters provided to the application
 */
case class TopPagesByGender(spark: SparkSession, params: Params) extends BaseTransform(spark, params) {
  private val DefaultNumberOfTopPages = 10
  private var pageViewsWatermark: String = _
  private var usersWatermark: String = _
  import spark.implicits._

  override def transformStream(dataframes: Option[Map[String, DataFrame]]): Option[DataFrame] = {
    dataframes match {
      case None => throw new IllegalArgumentException("Dataframe to transform is None")
      case Some(dfList) => {
        println(s"---- List of DFS: ${dfList.keys.map(k => k).mkString(",")}")
        val pageViewsDF = dfList.get(PageViewsStream.PageViewsSchema.toString).get
        val usersDF = dfList.get(PageViewsStream.UsersSchema.toString).get

        params.eventTimeFieldPerTopic match {
          case None => throw new IllegalArgumentException("Field for watermark expected in parameters, but None found")
          case Some(fields) => (fields.get(PageViewsStream.PageViewsSchema.toString), fields.get(PageViewsStream.UsersSchema.toString)) match {
            case (None, _) => throw new Exception("Watermark for one required topic was not specified")
            case (_, None) => throw new Exception("Watermark for one required topic was not specified")
            case (Some(pageViewsField), Some(usersField)) => {
              this.pageViewsWatermark = pageViewsField
              this.usersWatermark = usersField
            }
          }
        }

        Some(
          pageViewsDF
          .join(usersDF,
            pageViewsDF("userid") === usersDF("userid") &&
              pageViewsDF(this.pageViewsWatermark) >= usersDF(this.usersWatermark) &&
              pageViewsDF(this.pageViewsWatermark) <= usersDF(this.usersWatermark) + functions.expr(s"interval ${params.delayPerTopic.get.get(PageViewsStream.UsersSchema.toString).head} seconds")
          ).select(
            usersDF("userid").as("userid"),
            pageViewsDF("viewtime").as("viewtime"),
            pageViewsDF(this.pageViewsWatermark),
            usersDF("gender").as("gender"),
            pageViewsDF("pageid").as("pageid")
          ).groupBy(
            col("gender"),
            window(col(this.pageViewsWatermark), s"${params.windowDuration} seconds", s"${params.slidingInterval} seconds"),
            col("pageid")
          ).agg(
            sum(col("viewtime")).as("total_viewtime"), functions.approx_count_distinct(col("userid")).as("distinct_userid_count")
          )
        )

      }
    }
  }

  override def transformBatch(dataFrame: Option[DataFrame]): Option[DataFrame] = {
    dataFrame match {
      case None => throw new IllegalArgumentException("Expected dataframe to transform in microbatch, but None found")
      case Some(preparedDF) => {
        val windowSpecPageId = Window.partitionBy("gender").orderBy(col("total_viewtime").desc)
        Some(
          preparedDF
            .withColumn(
              "page_pos", row_number().over(windowSpecPageId)
            ).where(col("page_pos") <= functions.lit(this.params.topPagesNumber.getOrElse(this.DefaultNumberOfTopPages)))
            .select(to_avro(functions.struct(preparedDF.columns.map(col _): _*).alias("value")))
        )
      }
    }
  }
}
