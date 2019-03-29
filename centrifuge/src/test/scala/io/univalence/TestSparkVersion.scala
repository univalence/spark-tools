package io.univalence

import org.apache.spark.SparkContext
import org.scalatest.FunSuite

class TestSparkVersion extends FunSuite {

  test("version") {
    assert(SparkContext.getClass.getPackage.getImplementationVersion == "2.1.1")
  }

}
