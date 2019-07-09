package io.univalence.parka

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuiteLike

class ParkaTest extends FunSuiteLike {

  val ss = SparkSession.builder().appName("Parka")
    .master("local[*]").getOrCreate()

  import ss.implicits._

  ignore("this is the first test"){
    val from = Seq(Element("0", 92233720368547758L), Element("1", 922337203685477580L), Element("2", 9223372036854775807L)).toDS()
    val to = Seq(Element("0", 92233720368547759L), Element("1", 822337203685477580L), Element("2", 9223372036854775807L)).toDS()

    Parka.apply(from, to)("key")
  }
}

case class Element(key: String, value: Long)
