package com.joyn.challenge.stream.transformation

import com.joyn.challenge.stream.config.Params
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseTransform(spark: SparkSession, params: Params) {
  import spark.implicits._

  /**
   * Write the transformations required to the input dataframes here
   * @param dataframes input dataframes
   * @return transformed dataframe
   */
  def transformStream(dataframes: Option[Map[String, DataFrame]]): Option[DataFrame]

  /**
   * Some limitations in the transformation or custom actions might be required,
   * use this method to implement final transformations required, that will be executed in micro-batches.
   * Normally, invoked by the Writer.
   * @param dataFrame corresponding to a micro-batch
   * @return possibly transformed dataframe
   */
  def transformBatch(dataFrame: Option[DataFrame]): Option[DataFrame]
}
