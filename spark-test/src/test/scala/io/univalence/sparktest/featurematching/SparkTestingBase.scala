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
  ignore("test RDDComparisons") {
    val expectedRDD = sc.parallelize(Seq(1, 2, 3))
    val resultRDD   = sc.parallelize(Seq(3, 2, 1))

    //TODO implements compareRDD
    //TODO implements assertRDDEquals(...)
    /*
      assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
      assert(None === compareRDDWithOrder(expectedRDD, resultRDD)) // Fail
      assertRDDEquals(expectedRDD, resultRDD) // succeed
      assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail
   */
  }

  //https://github.com/holdenk/spark-testing-base/wiki/DataFrameSuiteBase
  test("test DataFrame Comparison") {
    val input1 = sc.parallelize(List(1, 2, 3)).toDF

    input1 assertEquals input1 // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDF
    intercept[AssertionError] {
      input1 assertEquals input2 // not equal
    }
  }

  ignore("test DataFrame Comparison with precision") {
    //TODO Implement approximative comparison
    /*
    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDF
    val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDF
    assertDataFrameApproximateEquals(input1, input2, 0.11) // equal

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(input1, input2, 0.05) // not equal
    }
   */
  }

  //https://github.com/holdenk/spark-testing-base/wiki/DatasetSuiteBase
  test("test Dataset equality") {
    val input1 = sc.parallelize(List(1, 2, 3)).toDS
    input1 assertEquals input1 // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDS
    intercept[AssertionError] {
      input1 assertEquals input2 // not equal
    }
  }

  ignore("test Dataset Comparison with precision") {

    //TODO implements
    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDS
    val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDS
    /*
    assertDatasetApproximateEquals(input1, input2, 0.11) // equal

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetApproximateEquals(input1, input2, 0.05) // not equal
    }
   */
  }

  //https://github.com/holdenk/spark-testing-base/wiki/RDDGenerator
  //https://github.com/holdenk/spark-testing-base/wiki/DataFrameGenerator
  //https://github.com/holdenk/spark-testing-base/wiki/Dataset-Generator
  //Not supported, IMO (Jon) doesn't really make sens for real use cases

}
