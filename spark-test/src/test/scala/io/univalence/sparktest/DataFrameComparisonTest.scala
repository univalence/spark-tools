package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StructField, StructType }
import org.scalatest.FunSuite

class DataFrameComparisonTest extends FunSuite with SparkTest {

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  // TODO : unordered
  ignore("should assertEquals unordered between equal DF") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(3, 2, 1).toDF("id")

    dfUT.assertEquals(dfExpected)
  }

  // TODO : unordered
  ignore("should not assertEquals unordered between DF with different contents") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(2, 1, 4).toDF("id")

    assertThrows[SparkTestError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  test("should assertEquals ordered between equal DF") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 2, 3).toDF("id")

    dfUT.assertEquals(dfExpected)
  }

  test("should not assertEquals ordered between DF with different contents") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 3, 4).toDF("id")

    assertThrows[SparkTestError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  test("should not assertEquals between DF with different schema") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 2, 3).toDF("di")

    assertThrows[SparkTestError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  test("assertEquals (DF & Seq) : a DF and a Seq with the same content are equal") {
    val seq = Seq(1, 2, 3)
    val df = ss.createDataFrame(
      sc.parallelize(seq.map(Row(_))),
      StructType(List(StructField("number", IntegerType, nullable = true)))
    )

    df.assertEquals(seq)
  }

  test("assertEquals (DF & Seq) : a DF and a Seq with different content are not equal") {
    val df    = Seq(1, 3, 3).toDF("number")
    val seqEx = Seq(1, 2, 3)

    assertThrows[SparkTestError] {
      df.assertEquals(seqEx)
    }
  }

  /*test("assertEquals (DF & Map) : a DF and a Map with the same content are equal") {

  }*/

  /*test("shouldExists (DF) : at least one row should match the predicate") {
    val l = List(1, 2, 3)
    val df = ss.createDataFrame(
      sc.parallelize(l.map(Row(_))),
      StructType(List(StructField("number", IntegerType, nullable = true)))
    )
    //df.shouldExists((n : Int) => n > 2) // ca fonctionne pas non plus :(
    //df.shouldExists(_ > 2)
  }

  test("shouldExists : should throw an error if all the rows don't match the predicate") {
    val df = Seq(1, 2, 3).toDF()

    assertThrows[AssertionError] {
      df.shouldExists((n: Int) => n > 3)
    }
  }

  test("shouldForAll : all the rows should match the predicate") {
    val df = Seq(1, 2, 3).toDF()

    df.shouldForAll((n: Int) => n >= 1)
  }

  test("shouldForAll : should throw an error if one of the row does not match the predicate") {
    val df = Seq(1, 2, 3).toDF()

    assertThrows[AssertionError] {
      df.shouldForAll((n: Int) => n >= 2)
    }
  }*/

}
