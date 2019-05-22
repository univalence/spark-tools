package io.univalence.sparktest.featurematching

import io.univalence.sparktest.SparkTest
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

//from https://github.com/holdenk/spark-testing-base/wiki
class SparkTestingBase extends FunSuite with SparkTest {

  //SharedSparkContext
  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  //https://github.com/holdenk/spark-testing-base/wiki/RDDComparisons
  test("test RDDComparisons") {
    val expectedRDD = sc.parallelize(Seq(1, 2, 3))
    val resultRDD   = sc.parallelize(Seq(3, 2, 1))

    assert(None === expectedRDD.compareRDD(resultRDD))
    expectedRDD.assertRDDEquals(resultRDD)
    assert(Some((Some(1), Some(3))) === expectedRDD.compareRDDWithOrder(resultRDD))
    intercept[AssertionError] {
      expectedRDD.assertRDDEqualsWithOrder(resultRDD)
    }
  }

  //https://github.com/holdenk/spark-testing-base/wiki/DataFrameSuiteBase
  test("dataframe should be equal to itself") {
    val input1 = sc.parallelize(List(1, 2, 4)).toDF

    input1 assertEquals input1 // equal
  }

  test("dataframe should not be equal to a different dataframe") {
    val input1 = sc.parallelize(List(1, 2, 4)).toDF
    val input2 = sc.parallelize(List(2, 4, 1)).toDF

    intercept[AssertionError] {
      input1.assertEquals(input2, checkRowOrder = true) // not equal
    }
  }

  test("test DataFrame Comparison with precision") {
    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDF
    val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDF
    //assertDataFrameApproximateEquals(input1, input2, 0.11) // equal
    input1.assertApproxEquals(input2, 0.11) // equal
    intercept[AssertionError] {
      input1.assertApproxEquals(input2, 0.05) // not equal
    }
  }

  //https://github.com/holdenk/spark-testing-base/wiki/DatasetSuiteBase
  test("dataset should be equal to itself") {
    val input1 = sc.parallelize(List(1, 2, 3)).toDS

    input1 assertEquals input1 // equal
  }

  test("dataset should not be equal to a different dataset") {
    val input1 = sc.parallelize(List(1, 2, 3)).toDS
    val input2 = sc.parallelize(List(4, 5, 6)).toDS

    intercept[AssertionError] {
      input1 assertEquals input2 // not equal
    }
  }

  ignore("dataset should be equal to itself with precision") {
    //TODO implements
    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDS

    //    assertDatasetApproximateEquals(input1, input2, 0.11) // equal
  }

  ignore("dataset should not be equal to a different even with precision") {
    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDS
    val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDS

    //    intercept[org.scalatest.exceptions.TestFailedException] {
//      assertDatasetApproximateEquals(input1, input2, 0.05) // not equal
//    }
  }

  //https://github.com/holdenk/spark-testing-base/wiki/RDDGenerator
  //https://github.com/holdenk/spark-testing-base/wiki/DataFrameGenerator
  //https://github.com/holdenk/spark-testing-base/wiki/Dataset-Generator
  //Not supported, IMO (Jon) doesn't really make sens for real use cases

}
