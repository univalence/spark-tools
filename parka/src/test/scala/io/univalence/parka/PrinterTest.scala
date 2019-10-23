package io.univalence.parka
import java.io.{ BufferedWriter, File, FileWriter }

import io.univalence.sparktest.SparkTest
import java.sql.{ Date, Timestamp }

import io.circe.Json
import io.univalence.schema.SchemaComparator.SchemaError
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import org.scalactic.Prettifier
import org.scalatest.{ FunSuite, Tag, Transformer }

class PrinterTest extends FunSuite with SparkTest with HistogramTest {

  import MonoidGen._

  implicit val default: Prettifier = Prettifier.default

  //Turn status to false if you want to ignore these tests
  private val status: Boolean = false

  private def run(testName: String, testTags: Tag*)(testFun: => Unit) {
    status match {
      case true  => test(testName, testTags: _*)(testFun)
      case false => ignore(testName, testTags: _*)(testFun)
    }
  }

  run("Prettify test") {
    val left: Dataset[Element] = dataset(
      Element("0", 100),
      Element("1", 100),
      Element("2", 100),
      Element("3", 100),
      Element("4", 100),
      Element("5", 100),
      Element("6", 100),
      Element("7", 100),
      Element("8", 100),
      Element("9", 100)
    )
    val right: Dataset[Element] = dataset(
      Element("0", 0),
      Element("1", 10),
      Element("2", 20),
      Element("3", 30),
      Element("4", 40),
      Element("5", 50),
      Element("6", 60),
      Element("7", 70),
      Element("8", 80),
      Element("9", 90),
      Element("10", 100)
    )
    val result = Parka(left, right)("key").result
    val part   = Printer.printParkaResult(result)
    println(Part.toString(part))
  }

  run("Prettify test 2 column") {
    val left: Dataset[Element2] = dataset(
      Element2("0", 0, 0),
      Element2("1", 10, 0),
      Element2("2", 20, 0),
      Element2("3", 30, 10),
      Element2("4", 40, 0),
      Element2("5", 50, 20),
      Element2("6", 60, 0),
      Element2("7", 70, 30),
      Element2("8", 80, 0),
      Element2("9", 80, 30)
    )
    val right: Dataset[Element2] = dataset(
      Element2("0", 0, 0),
      Element2("1", 10, 0),
      Element2("2", 20, 10),
      Element2("3", 30, 10),
      Element2("4", 40, 20),
      Element2("5", 50, 20),
      Element2("6", 60, 30),
      Element2("7", 70, 30),
      Element2("8", 80, 40),
      Element2("9", 90, 40),
      Element2("10", 100, 50)
    )
    val result = Parka(left, right)("key").result
    val part   = Printer.printParkaResult(result)
    println(Part.toString(part))
  }

  run("Prettify test null") {
    val left = Seq(
      (1, "aaaa"),
      (2, "bbbb"),
      (3, null),
      (4, null)
    ).toDF("id", "str")

    val right = Seq(
      (1, null),
      (2, "bbbb"),
      (3, "cccc"),
      (4, null),
      (5, "eeee")
    ).toDF("id", "str")

    val result = Parka(left, right)("id").result
    val part   = Printer.printParkaResult(result)
    println(Part.toString(part))
  }

  run("Prettify test enum") {
    val left = Seq(
      (1, "FR"),
      (2, "FR"),
      (3, "DE"),
      (4, "DE")
    ).toDF("id", "str")

    val right = Seq(
      (1, "FR"),
      (2, "DE"),
      (3, "FR"),
      (4, "DE"),
      (5, "DE")
    ).toDF("id", "str")

    val result = Parka(left, right)("id").result
    val part   = Printer.printParkaResult(result)
    println(Part.toString(part))
  }
}
