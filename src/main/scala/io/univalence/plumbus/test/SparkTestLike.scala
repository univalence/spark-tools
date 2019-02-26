package io.univalence.plumbus.test

import org.apache.spark.sql.SparkSession

trait SparkTestLike {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .config("spark.ui.enabled", value = false)
      .getOrCreate()

}

object SparkTestLike extends SparkTestLike
