package io.univalence.parka

import cats.kernel.Monoid
import com.twitter.algebird.QTree
import io.circe.{ Decoder, Encoder, ObjectEncoder }
import io.univalence.parka.Delta.{ DeltaBoolean, DeltaLong, DeltaString }
import io.univalence.parka.Describe.{ DescribeCombine, DescribeLong }
import io.univalence.sparktest.SparkTest
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class ParkaTest extends FunSuite with SparkTest {

  private val l1 = 1L
  private val l2 = 2L
  private val l3 = 3L
  private val l4 = 4L
  private val l5 = 5L
  private val l6 = 6L

  def assertDistinct[T](t: T*): Unit =
    assert(t.distinct == t)

  assertDistinct(l1, l2, l3, l4, l5, l6)

  def assertIn(t: (Double, Double), value: Long): Unit =
    assert(t._1 <= value && value <= t._2)

  def assertHistoEqual(longHisto: Histogram, value: Long*): Unit = {
    val sorted = value.sorted
    if (sorted.isEmpty)
      assert(longHisto.count == 0)
    else if (sorted.size == 1) {
      assertIn(longHisto.quantileBounds(1), value.head)
    } else {
      sorted.zipWithIndex.foreach({
        case (v, index) =>
          assertIn(longHisto.quantileBounds(index.toDouble / (sorted.size - 1)), v)
      })
    }
  }

  test("test deltaString") {

    val left  = dataframe("{id:1, value:'a', n:1}", "{id:2, value:'b', n:2}")
    val right = dataframe("{id:1, value:'a', n:1}", "{id:2, value:'b2', n:3}")

    val result = Parka(left, right)("id").result
    println(ParkaPrinter.printParkaResult(result))

    assert(result.inner.countDiffByRow == Map(Seq("n", "value") -> 1, Nil -> 1))

    assertHistoEqual(result.inner.byColumn("value").asInstanceOf[DeltaString].error, 1)
  }

  test("test deltaLong") {
    val left: Dataset[Element] =
      dataset(Element("0", l1), Element("1", l2), Element("2", l3), Element("3", l4))
    val right: Dataset[Element] = dataset(Element("0", l5), Element("1", l6), Element("2", l3))

    val result = Parka(left, right)("key").result
    assert(result.inner.countRowEqual === 1L)
    assert(result.inner.countRowNotEqual === 2L)
    assert(result.inner.countDiffByRow === Map(Seq("value") -> 2, Nil -> 1))
    val diff                 = Seq(l1 - l5, l2 - l6).map(x => x * x).sum
    val deltaLong: DeltaLong = result.inner.byColumn("value").asInstanceOf[DeltaLong]

    assert(deltaLong.nEqual == 1)
    assert(deltaLong.nNotEqual == 2)

    assertHistoEqual(deltaLong.error, l1 - l5, l2 - l6, 0)
    //TODO ERROR
    //  assertHistoEqual(deltaLong.describe.left.value, l1, l2,l3)
    //  assertHistoEqual(deltaLong.describe.right.value, l5, l6,l3)

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

    val deltaBoolean: DeltaBoolean = result.inner.byColumn("value").asInstanceOf[DeltaBoolean]

    assert(deltaBoolean.tf == 1, "at tf")
    assert(deltaBoolean.tt == 1, "at tt")
    assert(deltaBoolean.ff == 2, "at ff")
    assert(deltaBoolean.ft == 1, "at ft")

    assert(deltaBoolean.nEqual == 3)
    assert(deltaBoolean.nNotEqual == 2)
  }

  test("derivation test") {
    import MonoidGen._
    val describe: Monoid[Describe] = MonoidGen.gen[Describe]

    assert(describe.empty == Describe.empty)

    assert(describe.combine(Describe(1), Describe(1)).isInstanceOf[DescribeLong])
    assert(describe.combine(Describe(1), Describe("a")).isInstanceOf[DescribeCombine])
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
    println(ParkaPrinter.printParkaResult(result))
  }

  test("Json Serde") {
    val left   = dataframe("{id:1, n:1}", "{id:2, n:2}")
    val right  = dataframe("{id:1, n:1}", "{id:2, n:3}")
    val pa     = Parka(left, right)("id")
    val paJson = ParkaAnalysisSerde.toJson(pa)
    assert(
      paJson.noSpaces == """{"datasetInfo":{"left":{"source":[],"nStage":0},"right":{"source":[],"nStage":0}},"result":{"inner":{"countRowEqual":1,"countRowNotEqual":1,"countDiffByRow":[{"key":["n"],"value":1},{"key":[],"value":1}],"byColumn":{"n":{"DeltaLong":{"nEqual":1,"nNotEqual":1,"describe":{"left":{"value":{"neg":null,"countZero":0,"pos":{"_1":0,"_2":2,"_3":2,"_4":{"_1":0,"_2":1,"_3":1,"_4":null,"_5":{"_1":1,"_2":0,"_3":1,"_4":null,"_5":null}},"_5":{"_1":1,"_2":1,"_3":1,"_4":{"_1":2,"_2":0,"_3":1,"_4":null,"_5":null},"_5":null}}}},"right":{"value":{"neg":null,"countZero":0,"pos":{"_1":0,"_2":2,"_3":2,"_4":{"_1":0,"_2":1,"_3":1,"_4":null,"_5":{"_1":1,"_2":0,"_3":1,"_4":null,"_5":null}},"_5":{"_1":1,"_2":1,"_3":1,"_4":null,"_5":{"_1":3,"_2":0,"_3":1,"_4":null,"_5":null}}}}}},"error":{"neg":{"_1":1,"_2":0,"_3":1,"_4":null,"_5":null},"countZero":1,"pos":null}}}}},"outer":{"countRow":{"left":0,"right":0},"byColumn":{}}}}"""
    )
    val paFromJson = ParkaAnalysisSerde.fromJson(paJson).right.get

    assert(pa == paFromJson)
  }
}

case class Element(key: String, value: Long)
case class Element2(key: String, value: Long, eulav: Long)
