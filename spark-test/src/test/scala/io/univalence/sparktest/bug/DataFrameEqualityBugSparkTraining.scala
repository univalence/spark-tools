package io.univalence.sparktest.bug

import io.univalence.sparktest.SparkTest
import org.scalatest.FunSuite

class DataFrameEqualityBugSparkTraining extends FunSuite with SparkTest {

  //TODO @Harrison
  ignore("select name and age inlined") {
    dataframe("""{name:"John",age:13, num:1}""").createTempView("df")
    ss.sql("select name,age from df").assertEquals(dataframe("""{name:"John",age:13}"""))
  }
}
