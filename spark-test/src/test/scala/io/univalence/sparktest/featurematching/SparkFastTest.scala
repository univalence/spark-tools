package io.univalence.sparktest.featurematching
import io.univalence.sparktest.SparkTest
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.scalatest.FunSuite

class SparkFastTest extends FunSuite with SparkTest {

  //SharedSparkContext
  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  ignore("test DataFrame equality") {

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

  test("unordered equality") {
    val sourceDF = Seq(
      "1",
      "5"
    ).toDF("number")

    val expectedDF = Seq(
      "5",
      "1"
    ).toDF("number")

    //assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false) // equal
  }

  ignore("ignore nullable flag equality") {
    val data = Seq(
      Row(1),
      Row(5)
    )

    val sourceDF = ss.createDataFrame(
      sc.parallelize(data),
      StructType(List(StructField("number", IntegerType, false)))
    )

    val expectedDF = ss.createDataFrame(
      sc.parallelize(data),
      StructType(List(StructField("number", IntegerType, true)))
    )

    //assertSmallDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
  }

  ignore("approximate dataframe equality") {
    val sourceData = Seq(
      Row(1.11),
      Row(5.22),
      Row(null)
    )

    val sourceDF = ss.createDataFrame(
      sc.parallelize(sourceData),
      StructType(List(StructField("number", DoubleType, true)))
    )

    val expectedData = Seq(
      Row(1.1),
      Row(5.2),
      Row(null)
    )

    val expectedDF = ss.createDataFrame(
      sc.parallelize(expectedData),
      StructType(List(StructField("number", DoubleType, true)))
    )

    // TODO
    //assertApproximateDataFrameEquality(sourceDF, expectedDF, 0.1)
  }

}
