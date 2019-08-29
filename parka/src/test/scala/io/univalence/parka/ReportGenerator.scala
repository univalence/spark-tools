package io.univalence.parka
import java.io.{ BufferedWriter, File, FileWriter }

import io.univalence.sparktest.SparkTest
import io.circe.Json
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class ReportGenerator extends FunSuite with SparkTest {
  def parkaAnalysisToJson(pa: ParkaAnalysis, name: String): Unit = {
    val file         = new File(s"parka/src/test/ressources/reports/$name.json")
    val bw           = new BufferedWriter(new FileWriter(file))
    val paJson: Json = Serde.toJson(pa)

    bw.write(paJson.toString())
    bw.close()
  }

  def generateValues(size: Int, start: Int, step: Int): Seq[Int] = step match {
    case s if s == 0 => Seq.fill(size)(start)
    case _           => Seq.range(start, start + size * step, step)
  }

  def generateDatasetOfElements(size: Int)(start: Int, step: Int): Dataset[Element] = {
    val values: RDD[Int] = ss.sparkContext.parallelize(generateValues(size, start, step))
    values.zipWithIndex().map { case (v, i) => Element(i.toString, v) }.toDS()
  }

  def generateDatasetOf2Elements(size: Int)(start: Int, step: Int)(start2: Int, step2: Int): Dataset[Element2] = {
    val values: RDD[(Int, Int)] =
      ss.sparkContext.parallelize(generateValues(size, start, step) zip generateValues(size, start2, step2))
    values.zipWithIndex().map { case ((v1, v2), i) => Element2(i.toString, v1, v2) }.toDS()
  }

  /**
    * [Number of cols]_[Number of rows LeftVSRight]_[Mathematics set for values]_[Inner/Outer/Both]
    */
  test("report same_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(10)(0, 10)
    val right: Dataset[Element] = generateDatasetOfElements(10)(0, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "same_values")
  }

  test("report positif_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(10)(100, 0)
    val right: Dataset[Element] = generateDatasetOfElements(10)(10, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "positif_values")
  }

  test("report negatif_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(10)(-100, 0)
    val right: Dataset[Element] = generateDatasetOfElements(10)(-10, -10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "negatif_values")
  }

  test("report all_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(10)(0, 0)
    val right: Dataset[Element] = generateDatasetOfElements(10)(-50, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "all_values")
  }

  test("report outer") {
    val left: Dataset[Element]  = generateDatasetOfElements(5)(0, 5)
    val right: Dataset[Element] = generateDatasetOfElements(10)(0, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "outer")
  }

  test("report large") {
    val left: Dataset[Element]  = generateDatasetOfElements(1000)(0, 5)
    val right: Dataset[Element] = generateDatasetOfElements(1000)(-1000, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "large")
  }

  test("report null") {
    val left = Seq(
      (1, "aaaa"),
      (2, "bbbb"),
      (3, null),
      (4, null)
    ).toDF("key", "value")
    val right = Seq(
      (1, null),
      (2, "bbbb"),
      (3, "cccc"),
      (4, null),
      (5, "eeee")
    ).toDF("key", "value")

    val pa = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "null")
  }

  test("report boolean") {
    val left = Seq(
      (1, true),
      (2, true),
      (3, false),
      (4, false)
    ).toDF("key", "value")
    val right = Seq(
      (1, false),
      (2, true),
      (3, true),
      (4, false),
      (5, true)
    ).toDF("key", "value")

    val pa = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "boolean")
  }

  test("report twocol_same_values") {
    val left: Dataset[Element2]  = generateDatasetOf2Elements(10)(0, 10)(-10, 20)
    val right: Dataset[Element2] = generateDatasetOf2Elements(10)(0, 10)(-10, 20)

    val pa = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "twocol_same_values")
  }

  test("report twocol_different_values") {
    val left: Dataset[Element2]  = generateDatasetOf2Elements(10)(-5, 5)(-10, 20)
    val right: Dataset[Element2] = generateDatasetOf2Elements(10)(-10, 20)(0, 10)

    val pa = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "twocol_different_values")
  }
}
