package io.univalence.sparktest

import org.scalatest.FunSuite

class GettingStartedTest extends FunSuite with SparkTest {

  test("some test") {
    val df1 = dataframe("{a:1, b:true}", "{a:2, b:false}")
    val df2 = dataframe("{a:1}", "{a:3}")

    assertThrows[SparkTestError] {
      /*
      The data set content is different :

      in value at a, 3 was diff to 2
      dataframe({a: 2, b: false})
      dataframe({a: 3})
       */
      df1.assertEquals(df2)
    }
  }

  test("some test with custom configuration") {
    val df1 = dataframe("{a:1, b:true}")
    val df2 = dataframe("{a:1, c:false}")

    withConfiguration(failOnMissingExpectedCol = false, failOnMissingOriginalCol = false)({ df1.assertEquals(df2) })
  }
}

case class A(a: Int)
