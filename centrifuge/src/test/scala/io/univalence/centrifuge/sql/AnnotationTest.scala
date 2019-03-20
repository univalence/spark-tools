package io.univalence.centrifuge.sql

import io.univalence.centrifuge.Annotation
import io.univalence.centrifuge.AnnotationSql
import io.univalence.centrifuge.Result
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

case class Person(name: String, age: Int)

case class PersonWithAnnotations(name: String, age: Int, annotations: Seq[Annotation])

case class BetterPerson(name: String, age: Option[Int])

object AnnotationTest {
  def to_age(i: Int): Result[Int] = {} match {
    case _ if i < 0    => Result.fromError("BELOW_ZERO")
    case _ if i <= 13  => Result.fromWarning(i, "UNDER_13")
    case _ if i >= 130 => Result.fromError("OVER_130")
    case _             => Result.pure(i)
  }

  def non_empty_string(str: String): Result[String] =
    str match {
      //case None => Result.fromError("NULL_VALUE")
      case "" => Result.fromError("EMPTY_STRING")
      case _  => Result.pure(str)
    }

}

class AnnotationTest extends FunSuite with BeforeAndAfterAll {

  val ss: SparkSession = SparkSession
    .builder()
    .config("spark.default.parallelism", 2)
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.ui.enabled", false)
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  import ss.implicits._

  val joe     = Person("Joe", 14)
  val timmy   = Person("Timmy", 8)
  val invalid = Person("", -1)

  val onePersonDf = ss.sparkContext.makeRDD(Seq(joe)).toDF()

  test("quickstart") {

    val joe     = Person("Joe", 14)
    val timmy   = Person("Timmy", 8)
    val invalid = Person("", -1)

    import io.univalence.centrifuge.implicits._
    import ss.implicits._

    //val ss:SparkSession = ...
    val ds = ss.sparkContext.makeRDD(Seq(joe, timmy, invalid)).toDS()
    ds.createOrReplaceTempView("personRaw")

    //register a transformation with data quality
    ss.registerTransformation[Int, Int](
      "checkAge", {
        case i if i < 0   => Result.fromError("INVALID_AGE")
        case i if i < 13  => Result.fromWarning(i, "UNDER_13")
        case i if i > 140 => Result.fromError("OVER_140")
        case i            => Result.pure(i)
      }
    )

    ss.sql("select name, checkAge(age) as age, age as ageRaw from personRaw")
    //.show(false)

    /*
+-----+----+------+
|name |age |ageRaw|
+-----+----+------+
|Joe  |14  |14    |
|Timmy|8   |8     |
|     |null|-1    |
+-----+----+------+
     */

    /*
    ss.sql("select name, checkAge(age) as age from personRaw").as[Person].collect().foreach(//println()
    Caused by: java.lang.NullPointerException: Null value appeared in non-nullable field:
      - field (class: "scala.Int", name: "age")
    - root class: "io.univalence.centrifuge.sql.Person"
     */
    //case class BetterPerson(name:String,age:Option[Int])

    ss.sql("select name, checkAge(age) as age from personRaw")
      .as[BetterPerson]
    //.collect()
    //.foreach(//println()
    /*
BetterPerson(Joe,Some(14))
BetterPerson(Timmy,Some(8))
BetterPerson(,None)
     */

    ss.sql("select name, checkAge(age) as age from personRaw").includeAnnotations
    //.show(false)

    /*
+-----+----+--------------------------------------------+
|name |age |annotations                                 |
+-----+----+--------------------------------------------+
|Joe  |14  |[]                                          |
|Timmy|8   |[[UNDER_13,age,WrappedArray(age),false,1]]  |
|     |null|[[INVALID_AGE,age,WrappedArray(age),true,1]]|
+-----+----+--------------------------------------------+
     */
    /*
++-----+----+------------------------------------------------+
+|name |age |annotations                                     |
+|     |    |message     |onField |fromFields |isError |count|
++-----+----+------------+--------+-----------+--------+-----+
+|Joe  |14  |                  ---                           |
+|Timmy|8   |UNDER_13   |age     |age        |false   |1    |
+|     |null|INVALID_AGE |age     |age        |true    |1    |
+|  ------  |EMPTY_STRING|name    |name1      |true    |1    |
+|  ------  |     ---------       |name2      |    ----      |
++-----+----+------------+--------+-----------+--------+-----+
   */

  }

  import io.univalence.centrifuge.implicits._

  test("explore") {

    val select = onePersonDf.select("*")

    //println((select.queryExecution.toString())
    select.queryExecution.logical.expressions.foreach(x => {
      //println((x)
      //println((x.prettyName)
    })

  }

  test("basic") {
    val p =
      onePersonDf.includeAnnotations.as[PersonWithAnnotations].collect().head

    assert(p.annotations.isEmpty)
    assert(p.name == "Joe")
    assert(p.age == 14)
  }

  test("with one annotation") {
    ss.registerTransformation("to_age", AnnotationTest.to_age)
    ss.sparkContext
      .makeRDD(Seq(Person("Joe", 12)))
      .toDS()
      .createTempView("person")

    val p: Option[(Int, Seq[Annotation])] = ss
      .sql("select to_age(age) as age from person")
      .includeAnnotations
      .as[(Int, Seq[Annotation])]
      .collect()
      .headOption

    assert(p.isDefined)
    assert(p.get._1 == 12)
    assert(
      p.get._2.toList == List(
        Annotation(
          message    = "UNDER_13",
          isError    = false,
          count      = 1,
          onField    = Some("age"),
          fromFields = Vector("age")
        )
      )
    )
  }

  test("with more annotations (same field)") {
    ss.registerTransformation("to_age", AnnotationTest.to_age)
    ss.sparkContext
      .makeRDD(Seq(Person("Joe", 12)))
      .toDS()
      .createOrReplaceTempView("person")

    val p: Option[(Int, Seq[AnnotationSql])] = ss
      .sql("select to_age(to_age(age) + to_age(age) - to_age(age)) as age from person")
      .includeAnnotations
      .as[(Int, Seq[AnnotationSql])]
      .collect()
      .headOption

    assert(p.isDefined)
    assert(p.get._1 == 12)
    assert(
      p.get._2.toSet == Set(
        AnnotationSql("UNDER_13", isError = false, count = 4, onField = "age", fromFields = Vector("age"))
      )
    )
  }

  test("multicol") {

    ss.registerTransformation("to_age", AnnotationTest.to_age)
    ss.registerTransformation("non_empty", AnnotationTest.non_empty_string)

    ss.sparkContext
      .makeRDD(Seq(Person("", -1)))
      .toDS()
      .createOrReplaceTempView("person")

    val res = ss
      .sql("select to_age(age) as person_age, non_empty(name) as person_name from person")
      .includeAnnotations
      .as[(Option[Int], Option[String], Seq[AnnotationSql])]
      .collect()
      .head

    assert(res._1.isEmpty)
    assert(res._2.isEmpty)
    assert(
      res._3.toSet == Set(
        AnnotationSql(
          msg        = "BELOW_ZERO",
          isError    = true,
          count      = 1,
          onField    = "person_age",
          fromFields = Vector("age")
        ),
        AnnotationSql(
          msg        = "EMPTY_STRING",
          isError    = true,
          count      = 1,
          onField    = "person_name",
          fromFields = Vector("name")
        )
      )
    )
  }

  test("") {}

  test("ajout du flag") {}

  test("ajout des causality cols") {}
  test("ajout du transformation name") {}

  test("group by") {
    ss.registerTransformation("non_empty", AnnotationTest.non_empty_string)

    ss.sparkContext
      .makeRDD(Seq("" -> "", "" -> "a", "" -> "b"))
      .toDF()
      .createOrReplaceTempView("togroup")

    val agg = ss.sql(
      "select non_empty(_1) as f,FIRST(non_empty(_2),true) as s, count(*) as c from togroup group by non_empty(_1)"
    )
    val (a, b, c, as) = agg.includeAnnotations
      .as[(Option[String], String, Long, Seq[AnnotationSql])]
      .collect()
      .head

    assert(as.size == 2)

  }
  test("join") {

    ss.sparkContext
      .makeRDD(Seq(("a", 1, "aaa"), ("b", 2, "")))
      .toDF()
      .createOrReplaceTempView("a")

    ss.sparkContext
      .makeRDD(Seq((1, "", "a")))
      .toDF()
      .createOrReplaceTempView("entity")

    ss.sql("select _1 as id, _2 as nb, non_empty(_3) as name from a")
      .createOrReplaceTempView("a_validated")

    assert(
      ss.sql("select * from a_validated")
        .includeAnnotations
        .select("annotations")
        .as[Seq[AnnotationSql]]
        .collect()
        .toSeq
        .exists(_.nonEmpty)
    )

    ss.sql("select _1 as id, non_empty(_2) as name, _3 as a_id from entity")
      .createOrReplaceTempView("entity_validated")

    assert(
      ss.sql("select * from entity_validated")
        .includeAnnotations
        .select("annotations")
        .as[Seq[AnnotationSql]]
        .collect()
        .toSeq
        .exists(_.nonEmpty)
    )

    val df = ss.sql(
      "select *, concat(e.name,a.name) as supername from entity_validated e left join a_validated a on e.a_id = a.id"
    )

    //println((df.queryExecution.toString())

    //println((df.queryExecution.analyzed.toJSON)

    //df.includeAnnotations.show(false)
  }

  test("test function chain with option") {
    //
    ss.sql(
      "select *, concat(e.name,non_empty(a.name)) as supername from entity_validated e left join a_validated a on e.a_id = a.id"
    )
  }

  test("sub select") {
    ss.registerTransformation("non_empty", AnnotationTest.non_empty_string)

    ss.sparkContext
      .makeRDD(Seq(("", 1)))
      .toDS()
      .createOrReplaceTempView("subtable")

    val df = ss
      .sql("select * from (select non_empty(_1) as n1 from subtable where _2 == 1) t")
      .includeAnnotations

    val head = df.as[(String, Seq[AnnotationSql])].collect().head

    assert(head._2.nonEmpty)
  }

  test("toto") {

    ss.registerTransformation("to_age", AnnotationTest.to_age)

    ss.sparkContext
      .makeRDD(
        Seq(
          Person("Joe", 12),
          Person("Robert", -1),
          Person("Jane", 21)
        )
      )
      .toDS()
      .createOrReplaceTempView("person")

    ss.sql("select name as person_name, to_age(age) as person_age, age as age_raw from person").includeAnnotations
    // .show(false)

  }

  test("rename + ajout des sources") {}

  test("deltaQA 2 df") {
    val df1 =
      ss.sparkContext.makeRDD(Seq(("abc", 2, 3), ("def", 13, 17))).toDF()

    val df2 = ss.sparkContext.makeRDD(Seq(("abc", 5, 7))).toDF()

    val delta = df1.deltaWith(df2)

    assert(delta.cols.forall(_.hasDifference))
  }

  test("delta QA same DF") {
    val df = ss.sparkContext.makeRDD(Seq(("abc", 2, 3), ("def", 13, 17))).toDF()

    assert(df.deltaWith(df).cols.forall(!_.hasDifference))
  }

  test("support non numeric type in delta qa (equality + diff squared)") {}

}
