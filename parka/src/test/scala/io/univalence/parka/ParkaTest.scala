package io.univalence.parka

import cats.kernel.Monoid
import io.univalence.sparktest.SparkTest
import java.sql.{ Date, Timestamp }

import io.circe.Json
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.scalactic.Prettifier
import org.scalatest.FunSuite

class ParkaTest extends FunSuite with SparkTest with HistogramTest {

  import MonoidGen._

  implicit val default: Prettifier = Prettifier.default

  private val l1 = 1L
  private val l2 = 2L
  private val l3 = 3L
  private val l4 = 4L
  private val l5 = 5L
  private val l6 = 6L

  def assertDistinct[T](t: T*): Unit =
    assert(t.distinct == t)

  assertDistinct(l1, l2, l3, l4, l5, l6)

  test("test deltaString") {

    val left  = dataframe("{id:1, value:'a', n:1}", "{id:2, value:'b', n:2}")
    val right = dataframe("{id:1, value:'a', n:1}", "{id:2, value:'b2', n:3}")

    val result = Parka(left, right)("id").result
    println(Printer.printParkaResult(result))

    assert(result.inner.countDiffByRow == Map(Seq("n", "value") -> 1))
    assert(result.inner.countDiffByRow.values.sum === result.inner.countRowNotEqual)

    val histograms = result.inner.byColumn("value").error.histograms
    assertHistoEqual(histograms("levenshtein"), 1)
  }

  test("test deltaLong") {
    val left: Dataset[Element] =
      dataset(
        Element("0", l1),
        Element("1", l2),
        Element("2", l3),
        Element("3", l4)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", l5),
        Element("1", l6),
        Element("2", l3)
      )

    val analysis            = Parka(left, right)("key")
    val result: ParkaResult = analysis.result
    assert(result.inner.countRowEqual === 1L)
    assert(result.inner.countRowNotEqual === 2L)
    assert(result.inner.countDiffByRow === Map(Seq("value") -> 2))
    assert(result.inner.countDiffByRow.values.sum === result.inner.countRowNotEqual)

    val diff      = Seq(l1 - l5, l2 - l6).map(x => x * x).sum
    val deltaLong = result.inner.byColumn("value")

    assert(deltaLong.nEqual == 1)
    assert(deltaLong.nNotEqual == 2)

    val histogram = deltaLong.describe.left.histograms("value")

    assertHistoEqual(deltaLong.error.histograms("value"), l1 - l5, l2 - l6)

    assertHistoEqual(deltaLong.describe.left.histograms("value"), l1, l2, l3)
    assertHistoEqual(deltaLong.describe.right.histograms("value"), l5, l6, l3)

    assert(result.outer.countRow === Both(1L, 0L))
  }

  test("test deltaBoolean") {
    val left = dataframe("{id:1, value:true}",
                         "{id:2, value:true}",
                         "{id:3, value:false}",
                         "{id:4, value:false}",
                         "{id:5, value:false}",
                         "{id:6, value:false}")
    val right = dataframe("{id:1, value:false}",
                          "{id:2, value:true}",
                          "{id:3, value:false}",
                          "{id:4, value:true}",
                          "{id:5, value:false}")

    val result = Parka(left, right)("id").result
    println(Printer.printParkaResult(result))

    val deltaBoolean = result.inner.byColumn("value")
    val counts       = deltaBoolean.error.counts
    assert(counts("tf") == 1, "at tf")

    assert(deltaBoolean.asBoolean.get.tt == 1, "at tt")
    assert(deltaBoolean.asBoolean.get.ff == 2, "at ff")

    assert(counts("ft") == 1, "at ft")

    assert(deltaBoolean.nEqual == 3)
    assert(deltaBoolean.nNotEqual == 2)
  }

  test("deltaDate") {
    val left = Seq(
      (1, Date.valueOf("2019-07-19")),
      (2, Date.valueOf("2019-07-20")),
      (3, Date.valueOf("2019-07-21"))
    ).toDF("id", "date")

    val right = Seq(
      (1, Date.valueOf("2019-07-15")),
      (2, Date.valueOf("2019-07-15")),
      (3, Date.valueOf("2019-07-15")),
      (4, Date.valueOf("2019-07-15"))
    ).toDF("id", "date")

    val pr = Parka(left, right)("id").result
    assert(pr.inner.countRowEqual == 0)
    assert(pr.inner.countRowNotEqual == 3)
    val deltaDate = pr.inner.byColumn("date")
    assert(deltaDate.nEqual == 0)
    assert(deltaDate.nNotEqual == 3)
  }

  test("deltaTimestamp") {
    val left = Seq(
      (1, Timestamp.valueOf("2019-07-19 12:56:43")),
      (2, Timestamp.valueOf("2019-07-20 12:58:43")),
      (3, Timestamp.valueOf("2019-07-21 12:56:48"))
    ).toDF("id", "date")

    val right = Seq(
      (1, Timestamp.valueOf("2019-07-19 12:56:43")),
      (2, Timestamp.valueOf("2019-07-20 12:58:47")),
      (3, Timestamp.valueOf("2019-07-21 12:55:48"))
    ).toDF("id", "date")

    val pr = Parka(left, right)("id").result
    assert(pr.inner.countRowEqual == 1)
    assert(pr.inner.countRowNotEqual == 2)
    val deltaDate = pr.inner.byColumn("date")
    assert(deltaDate.nEqual == 1)
    assert(deltaDate.nNotEqual == 2)
  }

  test("Prettify test") {
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
    println(Printer.printParkaResult(result))
  }

  test("Json Serde") {
    val left: DataFrame           = dataframe("{id:1, n:1}", "{id:2, n:2}")
    val right: DataFrame          = dataframe("{id:1, n:1}", "{id:2, n:3}")
    val pa: ParkaAnalysis         = Parka(left, right)("id")
    val paJson: Json              = Serde.toJson(pa)
    val paFromJson: ParkaAnalysis = Serde.fromJson(paJson).right.get
    assert(pa == paFromJson)
  }

  test("test null") {
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
    println(Printer.printParkaResult(result))
  }
}

case class Element(key: String, value: Long)
case class Element2(key: String, value: Long, eulav: Long)