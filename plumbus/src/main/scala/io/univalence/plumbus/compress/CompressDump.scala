package io.univalence.plumbus.compress

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser

import scala.util.matching.Regex

case class CompressDumpArgs(
                             in: Seq[String] = Seq("tigrou"),
                             out: String = "bourriquet",
                             groupBy: Seq[String] = Seq("winnie"),
                             forceLocal: Boolean = false)

object CompressDump {

  lazy val groupExprColName: String = "groupByExpr"
  lazy val dtInFlightColName: String = "compressDumpDt"
  lazy val dtFinalColName: String = "compressDumpDts"
  lazy val rowsName: String = "xs"

  class MergeSampleLines(schema: StructType) extends UserDefinedAggregateFunction {
    override val inputSchema: StructType = StructType(schema.fields.filter(_.name != groupExprColName))

    private val newRowType: StructType = {
      val fields = inputSchema.fields.filter(_.name != dtInFlightColName).toSeq :+ StructField(dtFinalColName, ArrayType(StringType))
      StructType(fields)
    }

    override val dataType: DataType = ArrayType(newRowType)

    override val bufferSchema: StructType = StructType(Seq(StructField(rowsName, dataType)))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Vector.empty)
    }

    private lazy val dtPosInInput: Int = inputSchema.fields.size - 1

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      appendToBuffer(buffer, input.toSeq.dropRight(1), List(input.get(dtPosInInput).asInstanceOf[String]))
    }

    private def appendToBuffer(buffer: MutableAggregationBuffer, inputVals: Seq[Any], dtS: Seq[String]): Unit = {
      val rows = buffer.get(0).asInstanceOf[Seq[Row]]

      rows.zipWithIndex.find({
        case (r, _) ⇒
          r.toSeq.startsWith(inputVals)
      }) match {
        case Some((r, i)) ⇒
          buffer.update(0, rows.updated(i, new GenericRowWithSchema((r.toSeq.dropRight(1) :+
            (r.get(dtPosInInput).asInstanceOf[Seq[String]] ++ dtS)).toArray, r.schema)))

        case None ⇒
          val newRow = new GenericRowWithSchema((inputVals :+ dtS.asInstanceOf[Any]).toArray, newRowType)
          val updatedRows = rows :+ newRow
          buffer.update(0, updatedRows)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      for (r ← buffer2.get(0).asInstanceOf[Seq[Row]]) {
        appendToBuffer(buffer1, r.toSeq.dropRight(1), r.get(dtPosInInput).asInstanceOf[Seq[String]])
      }
    }

    override def evaluate(buffer: Row): Any = buffer.get(0)
  }

  def defaultSparkSession: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("dump compress")
      .getOrCreate()

  def compressUsingDF(dfs: Map[String, DataFrame], groupCols: Seq[String]): DataFrame = {
    import org.apache.spark.sql.functions._

    val firstDf: DataFrame = dfs.head._2
    val ss: SparkSession = firstDf.sparkSession
    val schema: StructType = firstDf.schema

    val df: DataFrame = dfs.map({ case (dt, dataFrame) ⇒ dataFrame.withColumn(dtInFlightColName, lit(dt)) }).reduce(_ union _)

    val fGroup: DataFrame = df.groupBy(df.schema.fieldNames.filter(_ != dtInFlightColName).map(col): _*)
      .agg(collect_list(dtInFlightColName).as(dtFinalColName))

    fGroup
      .groupBy(groupCols.map(col): _*)
      .agg(collect_list(struct(fGroup.schema.fieldNames.filterNot(groupCols.contains).map(col): _*)).as(rowsName))
      .orderBy(groupCols.map(col): _*)
  }

  def compressUsingDF2(dfs: Map[String, DataFrame], groupExpr: String): DataFrame = {

    import org.apache.spark.sql.functions._

    val firstDf: DataFrame = dfs.head._2
    val ss: SparkSession = firstDf.sparkSession
    val schema: StructType = firstDf.schema

    val df: DataFrame = dfs
      .map({ case (dt, dataFrame) ⇒ dataFrame.withColumn(dtInFlightColName, lit(dt)) })
      .reduce(_ union _)
      .withColumn(groupExprColName, expr(groupExpr))

    df
      .groupBy(groupExprColName)
      .agg(new MergeSampleLines(df.schema).apply(df.schema.fieldNames.filter(_ != groupExprColName).map(col): _*).as(rowsName))
    //.orderBy(groupExprColName)
  }

  def compressUsingRDD(dfs: Map[String, DataFrame], groupByExpr: String): DataFrame = {
    val ss: SparkSession = dfs.head._2.sparkSession

    import org.apache.spark.sql.functions._

    val df: DataFrame = dfs.map({ case (dt, dataFrame) ⇒ dataFrame.withColumn(dtInFlightColName, lit(dt)) }).reduce(_ union _)

    val preGrouped: DataFrame = df.withColumn(groupByExpr, expr(groupByExpr))
    val groupByExprPos: Int = preGrouped.schema.fieldIndex(groupByExpr)

    preGrouped.rdd.groupBy(_.get(groupByExprPos)).mapValues(it ⇒ {

      val xs: Seq[Row] = it.toSeq
      val schema: StructType = xs.head.schema

    })

    ???
  }

  def printChangedRows(df: DataFrame): Unit = {
    val pos: Int = df.schema.fieldNames.size - 1
    println(df.filter(r ⇒ r.get(pos).asInstanceOf[Seq[Row]].size > 1).count())
  }

  def main(args: Array[String]): Unit = {

    //scopt usage
    //sbt "run --in tinkywinky --out dipsy --groupBy laalaa,po"
    //alternative
    //sbt "run -i po -o tele -g tub,bies"
    val parser: OptionParser[CompressDumpArgs] = new scopt.OptionParser[CompressDumpArgs]("scopt") {
      head("scopt", "3.7.0")

      opt[Seq[String]]('i', "in").required().valueName("<inFile1>,<inFile2>,...").action((x, c) ⇒ c.copy(in = x))
        .text("Path of the read files")

      opt[String]('o', "out").required().valueName("<outFile>").action { (x, c) ⇒ c.copy(out = x) }
        .text("Path where the file will be written")

      opt[Seq[String]]('g', "groupBy").required().valueName("<columnName>").action((x, c) ⇒ c.copy(groupBy = x))
        .text("The groupBy column")

      opt[Boolean]("forceLocal").hidden().action({ (b, c) => c.copy(forceLocal = b) })

      help("help").text("Prints this help")
    }

    parser.parse(args, CompressDumpArgs()) match {
      case Some(config) ⇒ {

        val builder = SparkSession
          .builder()
          .config("rdd.compress", "true")
          .config("ui.enabled", "true")
          .config("serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("driver.memory", "2G")
          .config("executor.memory", "16G")
          .config("executor.cores", "1")
          //.config("dynamicAllocation.enabled", "true")
          //.config("dynamicAllocation.maxExecutors", "50")
          .config("yarn.executor.memoryOverhead", "4000")
          .config("default.parallelism", "32")
          .config("shuffle.service.enabled", "true")
          //.config("kryoserializer.buffer.max", "512M")
          .config("submit.deployMode", "client")

        val ss: SparkSession = (if (config.forceLocal) builder.master("local") else builder).getOrCreate()

        /*val ss = SparkSession.builder()
          .master("local[1]")
          .appName("tigrou")
          .config("spark.executor.memory", "25g")
          .config("spark.driver.memory", "1g")
          .config("spark.driver.maxResultSize", "0")
          .config("spark.sql.autoBroadcastJoinThreshold", "-1")
          //.config("spark.executor.memoryOverhead", "6g")
          .getOrCreate()*/
        ss.sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        ss.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 2001)

        //regex qui match les dates yyyymmdd
        val regex: Regex = "([12]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01]))".r

        //Faster than zipping
        val dfsMap: Map[String, DataFrame] = config.in.foldLeft[Map[String, DataFrame]](Map.empty[String, DataFrame])(
          (mapdatedf, path) ⇒ mapdatedf + ((regex.findAllIn(path).mkString, ss.read.parquet(path))))

        compressUsingDF(dfsMap, config.groupBy)
          .coalesce(1)
          .write
          .option("compression", "gzip")
          .mode("overwrite")
          .save(config.out)

        val compressedDf: DataFrame = ss.read.parquet(config.out)

        printChangedRows(compressedDf)

        ss.stop()

      }

      case None ⇒ println("No configuration (´_ﾉ`)")
    }

  }
}

