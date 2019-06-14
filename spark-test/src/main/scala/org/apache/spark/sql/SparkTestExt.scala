package org.apache.spark.sql

object SparkTestExt {

  /**
    * Check if a dataset is already cached or not
    * @param ds     The Dataset that may be in cache
    * @return
    */
  def isCached(ds: Dataset[_]): Boolean = {
    val ss           = ds.sparkSession
    val cacheManager = ss.sharedState.cacheManager
    cacheManager.lookupCachedData(ds).nonEmpty
  }
}
