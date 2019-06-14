package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.scalatest.FunSuite

class DataSetComparisonTest extends FunSuite with SparkTest {

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  // TODO unordered
  ignore("should assertEquals unordered between equal DS") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(3, 2, 1).toDS()

    dsUT.assertEquals(dsExpected)
  }

  // TODO unordered
  ignore("should not assertEquals unordered between DS with different contents") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(2, 1, 4).toDS()

    assertThrows[AssertionError] {
      dsUT.assertEquals(dsExpected)
    }
  }

  test("should assertEquals ordered between equal DS") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(1, 2, 3).toDS()

    dsUT.assertEquals(dsExpected)
  }

  test("should not assertEquals ordered between DS with different contents") {
    val dsUT       = Seq(1, 2, 3).toDS()
    val dsExpected = Seq(1, 3, 4).toDS()

    assertThrows[SparkTestError] {
      dsUT.assertEquals(dsExpected)
    }
  }

  test("should not assertEquals between DS with different schema") {
    val dsUT       = Seq(1, 2, 3).toDF("id").as[Int]
    val dsExpected = Seq(1, 2, 3).toDF("di").as[Int]

    assertThrows[SparkTestError] {
      dsUT.assertEquals(dsExpected)
    }
  }

  test("shouldExists : at least one row should match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    ds.shouldExists(_ > 2)
  }

  test("shouldExists : should throw an error if all the rows don't match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    assertThrows[AssertionError] {
      ds.shouldExists(_ > 3)
    }
  }

  test("shouldForAll : all the rows should match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    ds.shouldForAll(_ >= 1)
  }

  test("shouldForAll : should throw an error if one of the row does not match the predicate") {
    val ds = Seq(1, 2, 3).toDS()

    assertThrows[AssertionError] {
      ds.shouldForAll(_ >= 2)
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

  ignore("ignoreNullableFlag : two DataSet with different nullable should be equal if ignoreNullableFlag is true") {
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

    sourceDF.assertEquals(expectedDS)
  }

  // TODO : Add tests when not equal for comparisons with Seq
  test("assertEquals (DS & Seq) : a DS and a Seq with the same content are equal") {
    val seq = Seq(1, 2, 3)
    val ds  = seq.toDF("id").as[Int]

    ds.assertEquals(seq)
  }

  test("assertEquals (DS & Seq) : a DS and a Seq with different content are not equal") {
    val seq = Seq(1, 2, 3)
    val ds  = seq.toDF("id").as[Int]

    val seqEx = Seq(1, 3, 3)

    assertThrows[SparkTestError] {
      ds.assertEquals(seqEx)
    }
  }
}
