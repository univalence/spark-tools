package io.univalence.centrifuge.sql

import io.univalence.centrifuge.Result
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

object Explore {

  def to_age(i: Int): Result[Int] = {
    {} match {
      case _ if i < 0 ⇒ Result.fromError("BELOW_ZERO")
      case _ if i <= 13 ⇒ Result.fromWarning(i, "UNDER_13")
      case _ if i >= 130 ⇒ Result.fromError("OVER_130")
      case _ ⇒ Result.pure(i)
    }
  }

  def non_empty_string(str: String): Result[String] = {
    str match {
      //case None => Result.fromError("NULL_VALUE")
      case "" ⇒ Result.fromError("EMPTY_STRING")
      case _ ⇒ Result.pure(str)
    }
  }

  def main(args: Array[String]): Unit = {

    import io.univalence.centrifuge.implicits._

    val ss =
      SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    ss.registerTransformation("to_age", to_age)
    ss.registerTransformation("non_empty_string", non_empty_string)

    import ss.sqlContext.implicits._

    ss.sparkContext
      .makeRDD(Seq(Person("Joe", 12), Person("Joseph", 50), Person("", -1)))
      .toDS()
      .createTempView("person")

    val df = ss
      .sql(
        "select non_empty_string(name) as person_name, to_age(age) as person_age from person")
      .includeAnnotations

    //println((df.schema.toString())

    System.exit(0)

    df.toJSON.collect() //.foreach(//println()

    //println((df.queryExecution.toString())

    df.printSchema()

    df.includeSources

    System.exit(0)
    //println((df.queryExecution.analyzed.toJSON)

    val df2 = df

    //df2.show(false)

    df2.printSchema()
    df2.createTempView("toto")

    //BLOG IDEA, AUTOMATICALY FLATTEN STRUCTURE IN SPARK
    ss.sql(
      "select col.msg, col.isError, col.count, age,name from  (select explode(annotations) as col, age, name from toto) t")
    //.show(false)

    ss.sql("select non_empty_string(name) as person_name from person")
      .includeAnnotations
    //.show(false)
  }

}
