package io.univalence.sparktest

import ValueComparison._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuiteLike

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ValueComparisonTest extends FunSuiteLike with SparkTest {
  import io.univalence.typedpath._

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  test("A row should have no modification with itself") {
    val df = Seq(
      ("1", "2")
    ).toDF("set", "id")

    val value = fromRow(df.first())
    assert(compareValue(value, value).isEmpty)
  }

  // TODO : Null Pointer Exception
  ignore("Null Pointer Exception") {
    val df1 = Seq(
      (null, 2)
    ).toDF("set", "id")

    val df2 = Seq(
      (Array("3", "3"), 1)
    ).toDF("set", "id")

    println(compareValue(fromRow(df1.first), fromRow(df2.first)))
  }

  test("Add an ArrayType in an ArrayType") {

    val df1 = Seq(
      (Array(Array("2", "1")), 2)
    ).toDF("set", "id")

    val df2 = Seq(
      (Array(Array("2", "1"), Array("2", "5")), 2)
    ).toDF("set", "id")

    /**
      * in field set at index 1, WrappedArray(2, 5) was added
      *
      * in field set at index 0, WrappedArray(2, 4) was not equal to WrappedArray(2, 3)
      * in field set at index 1, WrappedArray(2, 5) was not equal to WrappedArray(2, 2)
      */

    assert(compareValue(fromRow(df1.first), fromRow(df2.first)) ==
      ArrayBuffer(ObjectModification(path"set/index1", AddValue(AtomicValue(mutable.WrappedArray.make(Array("2", "5")))))))
  }

  test("Remove a field") {
    val df1 = Seq(
      (Array("2", "2"), 2, 2)
    ).toDF("set", "id", "id2")

    val df2 = Seq(
      (Array("2", "2"), 2)
    ).toDF("set", "id")

    assert(compareValue(fromRow(df1.first), fromRow(df2.first)) ==
      ArrayBuffer(ObjectModification(path"id2",RemoveValue(AtomicValue(2)))))
  }

  test("Add a field") {
    val df1 = Seq(
      (Array("2", "2"), 2)
    ).toDF("set", "id")

    val df2 = Seq(
      (Array("2", "2"), 2, 4)
    ).toDF("set", "id", "id2")

    assert(compareValue(fromRow(df1.first), fromRow(df2.first)) ==
      ArrayBuffer(ObjectModification(path"id2",AddValue(AtomicValue(4)))))
  }

  test("Change a field type") {
    val data1 = Seq(
      Row("1")
    )

    val data2 = Seq(
      Row(1)
    )

    val df1 = ss.createDataFrame(
      sc.parallelize(data1),
      StructType(List(StructField("number", StringType, nullable = false)))
    )

    val df2 = ss.createDataFrame(
      sc.parallelize(data2),
      StructType(List(StructField("number", IntegerType, nullable = false)))
    )

    // Normal qu'il y ait une modification ?
    println(compareValue(fromRow(df1.first), fromRow(df2.first)))
  }
}
