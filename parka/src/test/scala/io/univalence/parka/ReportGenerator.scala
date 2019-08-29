package io.univalence.parka
import java.io.{ BufferedWriter, File, FileWriter }

import io.univalence.sparktest.SparkTest

import io.circe.Json
import org.apache.spark.sql.{ Dataset }
import org.scalatest.FunSuite

class ReportGenerator extends FunSuite with SparkTest {
  def parkaAnalysisToJson(pa: ParkaAnalysis, name: String): Unit = {
    val file         = new File(s"parka/src/test/ressources/reports/$name.json")
    val bw           = new BufferedWriter(new FileWriter(file))
    val paJson: Json = Serde.toJson(pa)

    bw.write(paJson.toString())
    bw.close()
  }

  def generateDatasetOfElements(start: Int, size: Int, step: Int): Dataset[Element] = {
    val keys: Seq[String] = Seq.range(0, size).map(_.toString)
    val values: Seq[Int] = step match {
      case s if s == 0 => Seq.fill(size)(start)
      case _           => Seq.range(start, start + size * step, step)
    }
    val elements: Seq[Element] = (keys zip values).map({ case (key, value) => Element(key, value) })
    elements.toDS()
  }

  /**
    * [Number of cols]_[Number of rows LeftVSRight]_[Mathematics set for values]_[Inner/Outer/Both]
    */
  test("report same_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(0, 10, 10)
    val right: Dataset[Element] = generateDatasetOfElements(0, 10, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "same_values")
  }

  test("report positif_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(100, 10, 0)
    val right: Dataset[Element] = generateDatasetOfElements(10, 10, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "positif_values")
  }

  test("report negatif_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(-100, 10, 0)
    val right: Dataset[Element] = generateDatasetOfElements(-10, 10, -10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "negatif_values")
  }

  test("report all_values") {
    val left: Dataset[Element]  = generateDatasetOfElements(0, 10, 0)
    val right: Dataset[Element] = generateDatasetOfElements(-50, 10, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "all_values")
  }

  test("report outer") {
    val left: Dataset[Element]  = generateDatasetOfElements(0, 5, 5)
    val right: Dataset[Element] = generateDatasetOfElements(0, 10, 10)
    val pa                      = Parka(left, right)("key")
    parkaAnalysisToJson(pa, "outer")
  }

  test("report large") {
    val left: Dataset[Element]  = generateDatasetOfElements(0, 1000, 5)
    val right: Dataset[Element] = generateDatasetOfElements(-1000, 1000, 10)
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
}
