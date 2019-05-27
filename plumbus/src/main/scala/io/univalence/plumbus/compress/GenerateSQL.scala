package io.univalence.plumbus.compress

import io.univalence.plumbus.compress.CompressDump._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object GenerateSQL {

  def displayLigneFreqOverPkPerDump(df: DataFrame): Unit = {
    val pos = df.schema.fieldNames.indexOf(rowsName)
    val pos2 = df.schema.fields(pos).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames
      .indexOf(dtFinalColName)

    val rddF: RDD[(String, Int)] = df.rdd.flatMap(_.get(pos).asInstanceOf[Seq[Row]].flatMap(_.get(pos2)
      .asInstanceOf[Seq[String]]).groupBy(x ⇒ x).mapValues(_.size))

    rddF.countByValue().foreach(println)
  }

  def generateView(schema: StructType, tablename: String): String = {
    val keyFields: Vector[String] = schema.fieldNames.toVector.filterNot(_ == rowsName)

    val fields: Vector[String] = schema
      .fields(schema.fieldIndex(rowsName))
      .dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
      .fieldNames.filterNot(_ == dtFinalColName).toVector

    val projectionFields: Seq[String] = keyFields ++ Seq(
      "minDt",
      "maxDt",
      "minDt_prev",
      "maxDt_prev",
      "minDt_prev IS NULL as isInit") ++ fields.flatMap(name ⇒ {
      val name_prev = name + "_prev"
      Seq(
        s"""(minDt_prev IS NOT NULL) AND
          (  ($name <> $name_prev )
            OR ($name_prev IS NULL AND $name IS NOT NULL )
            OR ($name IS NULL AND $name_prev IS NOT NULL )
            ) as ${name}_hasChanged""",
        name, name_prev)
    })

    s"""
       select
          ${projectionFields.mkString(",\n")}
       from
          $tablename tbl,
          (select
            lineT2.*,
            LAG(minDt) OVER (order by minDt) as minDt_prev,
            LAG(maxDt) OVER (order by minDt) as maxDt_prev,

            ${fields.map(name ⇒ s"LAG($name) OVER (order by minDt) as ${name}_prev").mkString(",\n")}

            from
            (select
               lineT1.*,
               minDt,
               maxDt

               from
                tbl.$rowsName as lineT1,
                (select
                  min(dts.item) as minDt,
                  max(dts.item) as maxDt

                  from lineT1.compressdumpdts as dts) as dtsE) as lineT2) as lineT3
    """
  }

  /*def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local[1]")
      .appName("tigrou")
      .config("spark.executor.memory", "25g")
      .config("spark.driver.memory", "1g")
      .config("spark.driver.maxResultSize", "0")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      //.config("spark.executor.memoryOverhead", "6g")
      .getOrCreate()
    import ss.implicits._

    val df = ss.read.parquet("/home/phong/IdeaProjects/gouache/CompressionLZParquet/yo8")
    displayLigneFreqOverPkPerDump(df)
    println(generateView(df.schema, "youpi"))
  }*/

}
