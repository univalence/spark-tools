package org.apache.spark.sql

object SparkTestExt {

  def isCached(ds: Dataset[_]): Boolean = {
    val ss           = ds.sparkSession
    val cacheManager = ss.sharedState.cacheManager
    cacheManager.lookupCachedData(ds).nonEmpty
  }
}
