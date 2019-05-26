package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructField, StructType }
import org.scalatest.FunSuiteLike

class SparkTestTest extends FunSuiteLike with SparkTest {

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  test("load Json from String") {
    //manage json option for jackson
    val df = dfFromJsonString("{a:1}", "{a:2}")

    df.as[Long].assertEquals(Seq(1L, 2L))

    df.as[Long].assertContains(1, 2)
    df.as[Long].assertContains(1)
    df.as[Long].assertContains(2)

    df.assertEquals(df)
  }

  ignore("load Json from String2") {
    //#Hard we need to use magnolia to solve this one and generate an expression encoder
    /*
    val df = dfFromJsonString("[{a:1},{a:2}]")

    case class A(a:Int)

    df.as[A].assertContains(A(1),A(2))
   */
  }

  /*
  TODO : manage spark logs, only warnings, no logs in info
  TODO : optimize spark for small load
   */

  test("load Json from file") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df   = dfFromJsonFile(path)

    assert(df.count == 3)
  }

  // ========================== DataFrame Tests ====================================

  test("should assertEquals unordered between equal DF") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(3, 2, 1).toDF("id")

    dfUT.assertEquals(dfExpected)
  }

  test("should not assertEquals unordered between DF with different contents") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(2, 1, 4).toDF("id")

    assertThrows[AssertionError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  test("should assertEquals ordered between equal DF") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 2, 3).toDF("id")

    dfUT.assertEquals(dfExpected, checkRowOrder = true)
  }

  test("should not assertEquals ordered between DF with different contents") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 3, 4).toDF("id")

    assertThrows[AssertionError] {
      dfUT.assertEquals(dfExpected, checkRowOrder = true)
    }
  }

  test("should not assertEquals between DF with different schema") {
    val dfUT       = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 2, 3).toDF("di")

    //TODO : Build an ADT for Errors, AssertionError is too generic
    assertThrows[AssertionError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  // TODO : Add tests when not equal for comparisons with Seq, List
  test("assertEquals (DF & Seq) : a DF and a Seq with the same content are equal") {
    val seq = Seq(1, 2, 3)
    val df = ss.createDataFrame(
      sc.parallelize(seq.map(Row(_))),
      StructType(List(StructField("number", IntegerType, nullable = true)))
    )

    df.assertEquals(seq)
  }

  test("assertEquals (DF & List) : a DF and a List with the same content are equal") {
    val l = List(1, 2, 3)
    val df = ss.createDataFrame(
      sc.parallelize(l.map(Row(_))),
      StructType(List(StructField("number", IntegerType, nullable = true)))
    )

    df.assertEquals(l)
  }

  // ========================== DataSet Tests ====================================

  test("should assertEquals unordered between equal DS") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(3, 2, 1).toDS()

    dsUT.assertEquals(dsExpected)
  }

  test("should not assertEquals unordered between DS with different contents") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(2, 1, 4).toDS()

    assertThrows[AssertionError] {
      dsUT.assertEquals(dsExpected)
    }
  }

  test("should assertEquals ordered between equal DS") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(1, 2, 3).toDS()

    dsUT.assertEquals(dsExpected, checkRowOrder = true)
  }

  test("should not assertEquals ordered between DS with different contents") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(1, 3, 4).toDS()

    assertThrows[AssertionError] {
      dsUT.assertEquals(dsExpected, checkRowOrder = true)
    }
  }

  test("shouldExists : at least one row should match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    ds.shouldExists(i => i > 2)
  }

  test("shouldExists : should throw an error if all the rows don't match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    assertThrows[AssertionError] {
      ds.shouldExists(i => i > 3)
    }
  }

  test("shouldForAll : all the rows should match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    ds.shouldForAll(i => i >= 1)
  }

  test("shouldForAll : should throw an error if one of the row does not match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    assertThrows[AssertionError] {
      ds.shouldForAll(i => i >= 2)
    }
  }

  test("assertContains : The dataset should contains all values") {
    val ds = Seq(1, 2, 3).toDS()

    ds.assertContains(1, 2)
  }

  test("assertContains : should throw an exception if the dataset does not contain at least one of the expected value") {
    val ds = Seq(1, 2, 3).toDS()

    assertThrows[AssertionError] {
      ds.assertContains(1, 2, 4)
    }
  }

  test("ignoreNullableFlag : two DataSet with different nullable should be equal if ignoreNullableFlag is true") {
    val sourceData = Seq(
      Row(1.11),
      Row(5.22)
    )
    val sourceDF = ss
      .createDataFrame(
        sc.parallelize(sourceData),
        StructType(List(StructField("number", DoubleType, nullable = true)))
      )
      .as[Double]

    val expectedDS = ss
      .createDataFrame(
        sc.parallelize(sourceData),
        StructType(List(StructField("number", DoubleType, nullable = false)))
      )
      .as[Double]

    sourceDF.assertEquals(expectedDS, ignoreNullableFlag = true)
  }

  // TODO : Add tests when not equal for comparisons with Seq, List
  test("assertEquals (DS & Seq) : a DS and a Seq with the same content are equal") {
    val seq = Seq(1, 2, 3)
    val ds  = seq.toDF("id").as[Int]

    ds.assertEquals(seq)
  }

  test("assertEquals (DS & List) : a DS and a List with the same content are equal") {
    val l  = List(1, 2, 3)
    val ds = l.toDF("id").as[Int]

    ds.assertEquals(l)
  }

  // ========================== RDD Tests ====================================
  // TODO : Add tests when not equal for comparisons with Seq, List
  test("assertEquals (RDD & Seq) : an RDD and a Seq with the same content are equal") {
    val seq = Seq(1, 2, 3)
    val rdd = sc.parallelize(seq)

    rdd.assertEquals(seq)
  }

  test("assertEquals (RDD & List) : an RDD and a List with the same content are equal") {
    val l   = List(1, 2, 3)
    val rdd = sc.parallelize(l)

    rdd.assertEquals(l)
  }
}
