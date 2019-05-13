package io.univalence.sparktest

import org.apache.spark.sql.SparkSession

trait SparkTestSession {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .config("spark.ui.enabled", value = false)
      .getOrCreate()

}

object SparkTestSession extends SparkTestSession
