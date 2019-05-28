package io.univalence.plumbus

import io.univalence.plumbus.compress.CompressDump
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import org.scalatest.FunSuite

class CompressDumpTest extends FunSuite {

  val ss: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .config("spark.default.parallelism", "1")
      .getOrCreate()

  import ss.implicits._

  test("compressUsingDF2") {
    val stringToRs: Map[String, Seq[R]] =
      Map(
        "dump1" -> Seq(
          R(1, "a", 1),
          R(2, "b", 22)
        ),
        "dump2" -> Seq(
          R(1, "a", 3),
          R(2, "b", 22)
        )
      )

    val df1: Dataset[(Int, Seq[RCompressed])] =
      CompressDump
        .compressUsingDF2(dfs = stringToRs.mapValues(s => ss.createDataset(s).toDF()), groupExpr = "id")
        .as[(Int, Seq[RCompressed])]

    /*val df2: DataFrame =
      CompressDump
        .compressUsingDF(dfs = stringToRs.mapValues(s ⇒ ss.createDataset(s).toDF()), groupCols = Seq("id"))

    println(df1.queryExecution.sparkPlan.treeString)
    println(df2.queryExecution.sparkPlan.treeString)*/

    val map: Map[Int, Seq[RCompressed]] = df1.collect().toMap

    assert(map(1).size == 2)
  }

}

case class R(id: Int, a: String, b: Int)

case class RCompressed(id: Int, a: String, b: Int, compressDumpDts: Seq[String])
