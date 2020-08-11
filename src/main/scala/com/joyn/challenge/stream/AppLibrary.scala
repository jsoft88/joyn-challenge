package com.joyn.challenge.stream

import java.util.concurrent.TimeUnit

import com.joyn.challenge.stream.config.CLIParams
import com.joyn.challenge.stream.factory.ApplicationFactory
import com.joyn.challenge.stream.jobs.kafka.PageViewsStream
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

sealed trait AppLibEntry

object AppLibrary {
  case object ChallengeApp extends AppLibEntry {
    override def toString: String = "challenge"
  }

  case object QuickJob extends AppLibEntry {
    override def toString: String = "nothing"
  }

  val AllApps = Seq(ChallengeApp)

  def main(args: Array[String]): Unit = {
    val cliParams = new CLIParams().buildCLIParams(args)
    ApplicationFactory.getApplicationInstance(cliParams.launchApp.getOrElse(QuickJob.toString), cliParams)
      .runStreamJob()
  }
}
