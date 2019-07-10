package io.univalence.parka

import io.univalence.sparktest.SparkTest
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class ParkaTest extends FunSuite with SparkTest {

  private val l1 = 92233720368547758L
  private val l2 = 922337203685477580L
  private val l3 = 9223372036854775807L
  private val l4 = 922337203685475807L
  private val l5 = 92233720368547759L
  private val l6 = 822337203685477580L

  def assertDistinct[T](t: T*): Unit =
    assert(t.distinct == t)

  assertDistinct(l1, l2, l3, l4, l5, l6)

  test("this is the first test") {
    val from: Dataset[Element] = dataset(Element("0", l1), Element("1", l2), Element("2", l3), Element("3", l4))
    val to: Dataset[Element]   = dataset(Element("0", l5), Element("1", l6), Element("2", l3))

    val result = Parka(from, to)("key")

    assert(result.deltaInfo.countRowEqual === 1L)
    assert(result.deltaInfo.countRowNotEqual === 2L)
    assert(result.outerInfo.countRow === Both(1L, 0L))
  }

  test("this is the second test") {
    val from: Dataset[Element2] =
      dataset(Element2("0", l1, l1), Element2("1", l2, l2), Element2("2", l3, l3), Element2("3", l4, l4))
    val to: Dataset[Element2] = dataset(Element2("0", l5, l5), Element2("1", l6, l6), Element2("2", l3, l3))

    val result = Parka(from, to)("key")

    assert(result.deltaInfo.countRowEqual === 1L)
    assert(result.deltaInfo.countRowNotEqual === 2L)
    assert(result.outerInfo.countRow === Both(1L, 0L))
  }
}

case class Element(key: String, value: Long)
case class Element2(key: String, value: Long, eulav: Long)
