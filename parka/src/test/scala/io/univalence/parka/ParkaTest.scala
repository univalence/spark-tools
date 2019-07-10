package io.univalence.parka

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuiteLike

class ParkaTest extends FunSuiteLike {

  val ss = SparkSession
    .builder()
    .appName("Parka")
    .master("local[*]")
    .getOrCreate()

  import ss.implicits._

  test("this is the first test") {
    val from = Seq(Element("0", 92233720368547758L),
                   Element("1", 922337203685477580L),
                   Element("2", 9223372036854775807L),
                   Element("3", 922337203685475807L)).toDS()
    val to = Seq(Element("0", 92233720368547759L),
                 Element("1", 822337203685477580L),
                 Element("2", 9223372036854775807L)).toDS()

    val result = Parka(from, to)("key")

    assert(result.deltaInfo.countRowEqual === 1L)
    assert(result.deltaInfo.countRowNotEqual === 2L)
    assert(result.outerInfo.countRow === Both(1L, 0L))
  }

  test("this is the second test") {
    val from = Seq(Element2("0", 92233720368547758L, 92233720368547758L),
      Element2("1", 922337203685477580L, 922337203685477580L),
      Element2("2", 9223372036854775807L, 9223372036854775807L),
      Element2("3", 922337203685475807L, 922337203685475807L)).toDS()
    val to = Seq(Element2("0", 92233720368547759L, 92233720368547759L),
      Element2("1", 822337203685477580L, 822337203685477580L),
      Element2("2", 9223372036854775807L, 9223372036854775807L)).toDS()

    val result = Parka(from, to)("key")

    assert(result.deltaInfo.countRowEqual === 1L)
    assert(result.deltaInfo.countRowNotEqual === 2L)
    assert(result.outerInfo.countRow === Both(1L, 0L))
  }
}

case class Element(key: String, value: Long)
case class Element2(key: String, value: Long, eulav: Long)
