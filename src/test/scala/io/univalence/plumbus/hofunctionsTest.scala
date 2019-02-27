package io.univalence.plumbus

import io.univalence.plumbus.test.SparkTestLike
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.FunSuiteLike
import org.scalatest.Matchers

class hofunctionsTest extends FunSuiteLike with SparkTestLike with Matchers {

  import spark.implicits._
  import hofunctions.implicits._

  val dataset: DataFrame =
    spark
      .createDataset(
        Seq(
          "1" -> Seq(
            Person("John", 32),
            Person("Mary", 31)
          ),
          "2" -> Seq(
            Person("Fred", 42),
            Person("Elsa", 40),
            Person("Daryll", 10)
          )
        )
      )
      .toDF("id", "family")

  test("should |> over untyped id in a a dataframe") {
    val res = dataset.select($"id" |> ((id: String) => id.toInt))

    assert(res.as[Int].collect() sameElements Array(1, 2))
  }

  test("should |> over typed id in a a dataframe") {
    val res = dataset.select($"id".as[String] |> (_.toInt))

    assert(res.as[Int].collect() sameElements Array(1, 2))
  }

  test("should map over a Seq of Person in a dataframe") {
    val resultdf =
      dataset.select($"id", $"family".as[Seq[Person]].map(_.name).as("names"))

    val result =
      dataframeToMap(r => r.getAs[String]("id") -> r.getAs[Seq[String]]("names"))(resultdf)

    result("1") should contain inOrderOnly ("John", "Mary")
    result("2") should contain inOrderOnly ("Fred", "Elsa", "Daryll")
  }

  test("should filter over a Seq of Person in a dataframe") {
    val resultdf = dataset.select($"id", $"family".as[Seq[Person]].filter(_.age < 18).as("persons"))

    val result =
      dataframeToMap(
        r =>
          r.getAs[String]("id")
            -> hofunctions.serializeAndCleanValue(r.getAs[Seq[Person]]("persons")))(resultdf)

    result("1") shouldBe empty
    result("2") should contain only Person("Daryll", 10)
  }

  test("should flaMap over a Seq of Person in a dataframe") {
    val resultdf =
      dataset.select($"id",
                     $"family"
                       .as[Seq[Person]]
                       .flatMap(
                         p =>
                           if (p.age > 18)
                             Seq()
                           else
                             Seq(p, Person("Younger " + p.name, p.age - 5)))
                       .as("album"))

    val result =
      dataframeToMap(
        r =>
          r.getAs[String]("id")
            -> hofunctions.serializeAndCleanValue(r.getAs[Seq[Person]]("album")))(resultdf)

    result("1") shouldBe empty
    result("2") should contain inOrderOnly (Person("Daryll", 10), Person("Younger Daryll", 5))
  }

  private def dataframeToMap[A, B](f: Row => (A, B))(df: DataFrame): Map[A, B] =
    df.collect().map(f).toMap

}

case class Person(name: String, age: Int)
