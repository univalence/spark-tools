package io.univalence.sparktest.internal

import org.apache.spark.sql.{ Dataset, SparkTestExt }

object DatasetUtils {

  def cacheIfNotCached(ds: Dataset[_]): Unit =
    if (!isCached(ds)) ds.cache()

  def isCached(ds: Dataset[_]): Boolean =
    SparkTestExt.isCached(ds)
}
