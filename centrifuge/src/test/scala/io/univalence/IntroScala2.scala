package io.univalence

import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.util.Success
import scala.util.Try

object IntroScala2 {

  def juxt[A, B, C](f: A => B, g: A => C): A => (B, C) = a => (f(a), g(a))

  implicit class PipeOps[A](a: A) {
    def |>[B](f: A => B): B = f(a)
  }

  implicit class PairList[K, V](lkv: List[(K, V)]) {

    def groupByKey: List[(K, List[V])] =
      lkv.groupBy(_._1).mapValues(_.map(_._2)).toList

    def join[VV](ll: List[(K, VV)]): List[(K, (V, VV))] = {
      val l = lkv.map({ case (k, v) => (k, Left(v)) }) ++
        ll.map({ case (k, v) => (k, Right(v)) })
      l.groupBy(_._1)
        .mapValues(x => {

          val all             = x.map(_._2)
          val left: List[V]   = all.collect({ case Left(v) => v })
          val right: List[VV] = all.collect({ case Right(v) => v })

          (left, right) match {
            case (Nil, rxs) => ???
            case (lxs, Nil) => ???
            case _ =>
              for {
                l <- left
                r <- right
              } yield (l, r)
          }

        })
        .toList
        .flatMap(x => x._2.map(x._1 -> _))
    }
  }

  val l = List((1, "a"), (2, "b"))

  l.groupByKey

  PairList(l).groupByKey

  def main(args: Array[String]): Unit = {

    //println(("abc,def,ghi,jkl,mno".split(",") |> juxt(_(4), _(3)))

  }

}

case class Line1(id: Int, vals: Seq[String])

object Line1 {

  def fromString(s: String): Try[Line1] =
    Try {
      val Array(id, vals) = s.split(",")
      Line1(id.toInt, vals.split(" "))
    }
}

object IntroSpark {

  def main(args: Array[String]): Unit = {

    val ss: SparkSession =
      SparkSession.builder().master("local[*]").appName("toto").getOrCreate()

    import ss.implicits._

    val rdd1: RDD[String] =
      ss.sparkContext.makeRDD(Seq("1,abc def", "2,ghi jkl"))

    val rdd2: RDD[Try[Line1]] = rdd1.map(Line1.fromString)

    val rdd3: RDD[Line1] = rdd2.collect({ case Success(l1) => l1 })

    val rdd4: RDD[String] = rdd3.flatMap(x => x.vals)

    rdd3.toDS().coalesce(1).write.parquet("target/test-spark-line1")

    Thread.sleep(100000)

  }
}

case class Clickstream(
  action: String,
  campaign: String,
  cost: Long,
  domain: String,
  ip: String,
  session: String,
  timestamp: Long,
  user: String
)

object IntroSpark2 {

  def main(args: Array[String]): Unit = {
    val ss: SparkSession =
      SparkSession.builder().master("local[*]").appName("toto").getOrCreate()

    ss.sparkContext.setLogLevel("WARN")

    import ss.implicits._

    def dataDir(s: String) =
      s"/Users/jon/project/cloud9-docker/spark-notebook/data/$s"

    val df1: DataFrame = ss.read.json(dataDir("click-stream/clickstream.json"))

    val ds1: Dataset[Clickstream] = df1.as[Clickstream]

    ds1.schema
    val select: DataFrame = ds1.select("action")

    /*
    root
 |-- action: string (nullable = true)
 |-- campaign: string (nullable = true)
 |-- cost: long (nullable = true)
 |-- domain: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- session: string (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- user: string (nullable = true)
     */

    //df1.show(false)

    /*
root
 |-- category: string (nullable = true)
 |-- domain: string (nullable = true)
     */

    val df2 = ss.read.json(dataDir("click-stream/domain-info.json"))

    import org.apache.spark.sql.functions._

    val condition: Column = $"action" === "blocked"
    val cond2: Column     = df1("action") === "blocked"
    val cond3: Column     = expr("action = 'blocked'")

    df1.join(df2, df1("domain") === df2("domain"), "outer")

    val res = df1.join(df2, df1("domain") === df2("domain"), "outer")

    // res.withColumn("domain_",coalesce(df1("domain"),df2("domain"))).drop(df1("domain")).drop(df2("domain")).show(false)

    MyUDF.register(ss)

    //df1.withColumn("timestamp", expr("toTs(toTs(timestamp))")).show(false)

    df1.createTempView("clickstream")

    System.exit(0)

    ss.sql("select myToDate(timestamp) as ts from clickstream")

    type EndoDf = DataFrame => DataFrame

    val endoTsToDate: EndoDf = { df =>
      {
        if (df.schema.fields
              .collect({ case StructField("timestamp", LongType, _, _) => true })
              .nonEmpty) {
          df.withColumn("timestamp", expr("myToDate(timestamp)"))
        } else {
          df
        }
      }
    }

    val just1: EndoDf = df => df.withColumn("just1", new Column(Literal(1)))

    Seq(endoTsToDate, just1).reduce(_.andThen(_))

    //endoTsToDate(endoTsToDate(endoTsToDate(df1))).show(false)

  }

}
