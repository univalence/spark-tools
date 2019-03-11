package org.apache.spark.sql

import io.univalence.centrifuge.{Annotation, Result}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Expression,
  GenericRow,
  GenericRowWithSchema,
  NamedExpression,
  ScalaUDF
}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.{SparkConf, SparkContext}

case class Person(name: String, age: Int)

case class Address(personName: String, street: String, postcode: String, city: String)

object Explore {

  def to_age(i: Int): Result[Int] = {
    {} match {
      case _ if i < 0    => Result.fromError("BELOW_ZERO")
      case _ if i <= 13  => Result.fromWarning(i, "UNDER_13")
      case _ if i >= 130 => Result.fromError("OVER_130")
      case _             => Result.pure(i)
    }
  }

  def non_empty_string(str: String): Result[String] = {
    str match {
      case "" => Result.fromError("EMPTY_STRING")
      case _  => Result.pure(str)
    }
  }

  def add_annotations(seq1: Seq[Annotation], seq2: Seq[Annotation]): Seq[Annotation] = seq1 ++ seq2

  def flatten_annotations[T](seq: Seq[Seq[T]]): Seq[T] =
    reduce_annotations(
      seq
        .filter(_ != null)
        .flatten
        .asInstanceOf[Seq[GenericRowWithSchema]])
      .asInstanceOf[Seq[T]]

  def reduce_annotations(seq: Seq[GenericRowWithSchema]): Seq[GenericRowWithSchema] = {
    seq
      .groupBy(x => (x.getAs[Any]("msg"), x.getAs[Any]("origin"), x.getAs[Any]("isError")))
      .map(t => {
        val toArray = t._2.head.toSeq.toArray
        toArray.update(t._2.head.fieldIndex("count"), t._2.map(_.getAs[Long]("count")).sum)
        new GenericRowWithSchema(toArray, t._2.head.schema)
      })
      .toSeq
  }

  def main(args: Array[String]): Unit = {

    val ss =
      SparkSession.builder().master("local[*]").appName("test").getOrCreate()

    val sc = ss.sparkContext

    val sQLContext = ss.sqlContext

    import sQLContext.implicits._

    sc.makeRDD(
        Seq(
          Person("john", -1),
          Person("",     13),
          Person("toto", 12)
        ))
      .toDF()
      .createOrReplaceTempView("person")

    sc.makeRDD(Seq(Address("toto", "", "75000", "Paris")))
      .toDF()
      .createTempView("address")

    sQLContext.udf.register("to_age",           to_age _)
    sQLContext.udf.register("non_empty_string", non_empty_string _)
    sQLContext.udf.register("add_annotations",  add_annotations _)
    sQLContext.udf
      .register("flatten_annotations", flatten_annotations[Annotation] _)

    val sql1 = sQLContext.sql("select to_age(age) as age, non_empty_string(name) as name from person")
    //sql1.show(false)

    val plan = sql1.queryExecution.optimizedPlan

    def pp(a: Any) = println(a.getClass + " : " + a)

    println(plan.toString())
    println("-----")

    val collect: Seq[Option[Expression]] = plan.expressions.collect({
      case Alias(child, colname) => {
        child match {
          case s @ ScalaUDF(f, dt, children, inputTypes, Some(name)) if name == "non_empty_string" =>
            Some(s)
          case _ => None
        }
      }
    })

    val head:   Expression      = collect.flatten.head
    val anncol: NamedExpression = Alias(head, "annotations")()
    val newPlan = plan match {
      case Project(projectList, child) =>
        Project(anncol :: projectList.toList, child)
    }

    val ndf = new Dataset[Row](sqlContext = sQLContext,
                               newPlan,
                               RowEncoder(ss.sessionState.executePlan(newPlan).analyzed.schema))

    ndf.show(false)

    println("-----")

    val sql = sQLContext.sql(
      """select age.value as age,
       name.value as name,
       add_annotations(age.annotations, name.annotations) as annotations
from (select to_age(age) as age, non_empty_string(name) as name from person) tmp
      """.stripMargin
    )

    println(sql.queryExecution.optimizedPlan.toString())
    println(sql.queryExecution.optimizedPlan.toJSON)

    System.exit(0)

    sql.createOrReplaceTempView("validated_person")
    sql.show(false)

    sQLContext
      .sql(
        "select AVG(age) as age_avg, flatten_annotations(collect_list(annotations)) as annotations from validated_person")
      .show(false)

    sQLContext
      .sql(
        """select personName,
         street.value as street,
          postcode.value as postcode,
          city.value as city,
          flatten_annotations(array(street.annotations,postcode.annotations,city.annotations)) as annotations
          from (select personName,non_empty_string(street) as street, non_empty_string(postcode) as postcode, non_empty_string(city) as city
          from address) as vd
      """.stripMargin
      )
      .createTempView("validated_address")

    sQLContext
      .sql(
        """select age,name,personName,street,postcode,city,flatten_annotations(array(vp.annotations,ad.annotations)) as annotations from validated_person as vp LEFT JOIN validated_address as ad ON vp.name = ad.personName""")
      .show(false)

    //person.age
    //person.name
    //address.street
    //transformation query rewrite
    //origin tracking
    //map function (null pass though (if one arg is null, and not explicitly check for, then the result is null, applicative style)
    //for free, no need to declare the function before hand
    //memoization ?
    //what happens in case of group by ? (collect annotations)
    //annotation model (message, origin data set, nb occur)
    //rejection tables
  }

}
