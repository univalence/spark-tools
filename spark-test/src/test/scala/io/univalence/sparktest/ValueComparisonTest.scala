package io.univalence.sparktest

import Value._
import org.scalatest.FunSuiteLike

class ValueComparisonTest extends FunSuiteLike with SparkTest {

  test("rows with two different arrayType should give some row modifications") {
    val df1 = Seq(
      (Array("2", "2"), 2),
      (Array("2", "3"), 2)
    ).toDF("set", "id")

    val df2 = Seq(
      (Array("3", "3"), 1),
      (Array("2", "4"), 1)
    ).toDF("set", "id")

    println(toStringDataframeModif(compareDataframe(df1, df2)))

    /*
      in field set at index 0, 3 was not equal to 2
      in field set at index 1, 3 was not equal to 2
      in field id, 1 was not equal to 2

      in field set at index 1, 4 was not equal to 3
      in field id, 1 was not equal to 2
     */
    assert(compareDataframe(df1, df2).nonEmpty)
  }

  // TODO : Null Pointer Exception
  ignore("Null Pointer Exception") {
    val df1 = Seq(
      (null, 2),
      (Array("2", "3"), 2)
    ).toDF("set", "id")

    val df2 = Seq(
      (Array("3", "3"), 1),
      (Array("2", "4"), 1)
    ).toDF("set", "id")

    println(toStringDataframeModif(compareDataframe(df1, df2)))
  }

  test("two arrayType ") {
    val df1 = Seq(
      (Array(Array("2", "1")), 2),
      (Array(Array("2", "3"), Array("2", "2")), 2)
    ).toDF("set", "id")

    val df2 = Seq(
      (Array(Array("2", "1"), Array("2", "5")), 2),
      (Array(Array("2", "4"), Array("2", "5")), 2)
    ).toDF("set", "id")

    /**
      * in field set at index 1, WrappedArray(2, 5) was added
      *
      * in field set at index 0, WrappedArray(2, 4) was not equal to WrappedArray(2, 3)
      * in field set at index 1, WrappedArray(2, 5) was not equal to WrappedArray(2, 2)
      */
    assert(compareDataframe(df1, df2).nonEmpty)
  }
}
