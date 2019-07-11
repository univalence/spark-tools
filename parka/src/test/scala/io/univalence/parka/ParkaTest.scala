package io.univalence.parka

import io.univalence.parka.Delta.DeltaLong
import io.univalence.parka.Describe.DescribeLong
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
    assert(deltaLong.describe.left.moments.m1 == ((l1 + l2 + l3).toDouble / 3))
    assert(deltaLong.describe.right.moments.m1 == ((l5 + l6 + l3).toDouble / 3))

    assert(deltaLong.error.m1 * deltaLong.error.m0 == (l1 + l2 + l3 - l5 - l6 - l3))

    assert(result.outer.countRow === Both(1L, 0L))
  }
}

case class Element(key: String, value: Long)
case class Element2(key: String, value: Long, eulav: Long)
