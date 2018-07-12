package io.univalence

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering.KMeans


object ExecutorTest {

  def main(args: Array[String]): Unit = {



    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(???)

  }

}
