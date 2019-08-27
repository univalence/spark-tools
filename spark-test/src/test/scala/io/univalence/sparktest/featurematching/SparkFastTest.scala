package io.univalence.sparktest.featurematching

import io.univalence.schema.SchemaComparator.SchemaError
import io.univalence.sparktest.SparkTest
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructField, StructType }
import org.scalatest.FunSuite

//https://github.com/MrPowers/spark-fast-tests
class SparkFastTest extends FunSuite with SparkTest {

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  test("test DataFrame equality") {
    val sourceDF = Seq(
      "jose",
      "li",
      "luisa"
    ).toDF("name")

    val expectedDF = Seq(
      "jose",
      "li",
      "luisa"
    ).toDF("name")

    //assertSmallDatasetEquality(sourceDF, expectedDF) // equal
    sourceDF.assertEquals(expectedDF)
  }

  test("column equality") {
    val df = Seq(
      ("Pierre", "Pierre"),
      ("Louis", "Louis"),
      (null, null),
      ("Jean", "Jean")
    ).toDF("name", "expected_name")

    //assertColumnEquality(df, "name", "expected_name")
    df.assertColumnEquality("name", "expected_name")
  }

  // TODO unordered
  ignore("unordered equality") {
    val sourceDF = Seq(
      "1",
      "5"
    ).toDF("number")

    val expectedDF = Seq(
      "5",
      "1"
    ).toDF("number")

    //assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false) // equal
    //by default SparkTest doesn't check for ordering
    sourceDF.assertEquals(expectedDF)
  }

  test("ignore nullable flag equality") {
    val data = Seq(
      Row(1),
      Row(5)
    )

    val sourceDF = ss.createDataFrame(
      sc.parallelize(data),
      StructType(List(StructField("number", IntegerType, nullable = false)))
    )

    val expectedDF = ss.createDataFrame(
      sc.parallelize(data),
      StructType(List(StructField("number", IntegerType, nullable = true)))
    )

    sourceDF.assertEquals(expectedDF)

    assertThrows[SchemaError] {
      withConfiguration(failOnNullable = true)(sourceDF.assertEquals(expectedDF))
    }
  }

  test("approximate dataframe equality") {
    val sourceData = Seq(
      Row(1.11),
      Row(5.22),
      Row(null)
    )

    val sourceDF = ss.createDataFrame(
      sc.parallelize(sourceData),
      StructType(List(StructField("number", DoubleType, nullable = true)))
    )

    val expectedData = Seq(
      Row(1.1),
      Row(5.2),
      Row(null)
    )

    val expectedDF = ss.createDataFrame(
      sc.parallelize(expectedData),
      StructType(List(StructField("number", DoubleType, nullable = true)))
    )

    //assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.1)
    sourceDF.assertApproxEquals(expectedDF, 0.1)
  }

}
