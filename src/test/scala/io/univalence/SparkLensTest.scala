package io.univalence


import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.{ ArrayType, StringType }
import org.apache.spark.sql.univalence.utils.SparkLens._
import org.scalatest.FunSuite

case class Toto(name: String, age: Int)

case class Tata(toto: Toto)

class SparkLensTest extends FunSuite {

  val conf: SparkConf = new SparkConf()
  conf.setAppName("yo")
  conf.setMaster("local[*]")

  implicit val ss: SparkSession = SparkSession.builder.config(conf).getOrCreate

  import ss.implicits._

  test("testLensRegExp change string") {
    assert(lensRegExp(ss.createDataFrame(Seq(Toto("a", 1))))({
      case ("name", StringType) ⇒ true
      case _                    ⇒ false
    }, { case (a: String, d) ⇒ Some(a.toUpperCase) }).as[Toto].first() == Toto("A", 1))
  }

  test("change Int") {
    assert(lensRegExp(ss.createDataFrame(Seq(Tata(Toto("a", 1)))))({
      case ("toto/age", _) ⇒ true
      case _               ⇒ false
    }, { case (a: Int, d) ⇒ Some(a + 1) }).as[Tata].first() == Tata(Toto("a", 2)))
  }

  ignore("change null to Some(Nil) and not None because it is already used xddd") {

    val df: DataFrame = ???

    lensRegExp(df)({
      case (_, ArrayType(_, _)) ⇒ true
      case _                    ⇒ false
    }, (a, b) ⇒ if (a == null) Some(Nil) else None)
  }

}

