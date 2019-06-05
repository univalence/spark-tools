package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class RDDComparisonTest extends FunSuite with SparkTest {
  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

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

  test("shouldExists (RDD) : at least one row should match the predicate") {
    val rdd = sc.parallelize(Seq(1, 2, 3))

    rdd.shouldExists(_ > 2)
  }

  test("shouldExists (RDD) : should throw an error if all the rows don't match the predicate") {
    val rdd = sc.parallelize(Seq(1, 2, 3))

    assertThrows[AssertionError] {
      rdd.shouldExists(_ > 3)
    }
  }

  test("shouldForAll (RDD) : all the rows should match the predicate") {
    val rdd = sc.parallelize(Seq(1, 2, 3))

    rdd.shouldForAll(_ >= 1)
  }

  test("shouldForAll (RDD) : should throw an error if one of the row does not match the predicate") {
    val rdd = sc.parallelize(Seq(1, 2, 3))

    assertThrows[AssertionError] {
      rdd.shouldForAll(_ >= 2)
    }
  }
}
