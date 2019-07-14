package io.univalence.parka

import io.univalence.parka.Delta.{ DeltaLong, DeltaString }
import io.univalence.parka.Histogram.LongHisto
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

  def assertHistoEqual(longHisto: LongHisto, value: Long*): Unit = {
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

  test("this is the first test") {
    val from: Dataset[Element] = dataset(Element("0", l1), Element("1", l2), Element("2", l3), Element("3", l4))
    val to: Dataset[Element]   = dataset(Element("0", l5), Element("1", l6), Element("2", l3))

    val result = Parka(from, to)("key").result

    assert(result.inner.countRowEqual === 1L)
    assert(result.inner.countRowNotEqual === 2L)
    assert(result.outer.countRow === Both(1L, 0L))
  }

  test("this is the second test") {
    val from: Dataset[Element2] =
      dataset(Element2("0", l1, l1), Element2("1", l2, l2), Element2("2", l3, l3), Element2("3", l4, l4))
    val to: Dataset[Element2] = dataset(Element2("0", l5, l5), Element2("1", l6, l6), Element2("2", l3, l3))

    val result = Parka(from, to)("key").result

    assert(result.inner.countRowEqual === 1L)
    assert(result.inner.countRowNotEqual === 2L)
    assert(result.outer.countRow === Both(1L, 0L))
  }

  test("test deltaString") {

    val left  = dataframe("{id:1, value:'a', n:1}", "{id:2, value:'b', n:2}")
    val right = dataframe("{id:1, value:'a', n:1}", "{id:2, value:'b2', n:3}")

    val result = Parka(left, right)("id").result

    assert(result.inner.countDiffByRow == Map(2 -> 1, 0 -> 1))

    assertHistoEqual(result.inner.byColumn("value").asInstanceOf[DeltaString].error, 1)
  }

  test("test deltaLong") {
    val left: Dataset[Element] =
      dataset(Element("0", l1), Element("1", l2), Element("2", l3), Element("3", l4))
    val right: Dataset[Element] = dataset(Element("0", l5), Element("1", l6), Element("2", l3))

    val result = Parka(left, right)("key").result
    assert(result.inner.countRowEqual === 1L)
    assert(result.inner.countRowNotEqual === 2L)
    assert(result.inner.countDiffByRow === Map(1 -> 2, 0 -> 1))
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
}

case class Element(key: String, value: Long)
case class Element2(key: String, value: Long, eulav: Long)
