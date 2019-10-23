package io.univalence.parka

import java.io.{ BufferedWriter, File, FileWriter }

import cats.kernel.Monoid
import io.circe.Json
import io.univalence.parka.MonoidGen._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import io.univalence.schema.SchemaComparator
import org.apache.spark.{ sql, HashPartitioner, Partitioner }

/**
  * Parka is a library that perform deltaQA in Spark.
  * Basically, it compares two datasets and shows us the difference between the two of them.
  *
  * This library serve one principal purpose, facilitate "Regression testing(https://en.wikipedia.org/wiki/Regression_testing)"
  * using a new approach. The idea behind Parka is quite simple, when you perform some updates of a program, you need to check
  * if you don't break anything on it. Generally we create unit test and that's cool! However, they just cover a part of our program
  * and basically they don't check if everything is ok on real data. Parka try to fix that problem. Instead of checking function's behaviours,
  * let's check data by applying on a same dataset the previous and the new update and comparing them to see if something weird
  * happens.
  */
object Parka {
  private val keyValueSeparator = "§"

  /**
    * Generate a [Describe] for each column of a particular row
    *
    * @param row            Row that need to be describe
    * @param keys           Primary key(s)
    * @return               Map of [Describe] with each column that are not primary key(s) as a the key
    */
  def describe(row: Row)(keys: Set[String]): Map[String, Describe] = {
    val fields = row.asInstanceOf[GenericRowWithSchema].schema.fieldNames

    fields
      .filterNot(keys)
      .map(name => name -> Describe(row.getAs[Any](name)))
      .toMap
  }

  /**
    * Generate [Outer] that describe a row without twin on the other Dataset
    *
    * @param row            Row from the one of the two Dataset
    * @param side           Right if the row come from the right Dataset otherwise Left
    * @param keys           Column's names of both Datasets that are considered as keys
    * @return               Outer information from one particular row
    */
  def outer(row: Row, side: Side)(keys: Set[String]): Outer = {
    @inline
    def valueAndZero[T: Monoid](value: T): Both[T] =
      side match {
        case Left  => Both(left = value, right                       = implicitly[Monoid[T]].empty)
        case Right => Both(left = implicitly[Monoid[T]].empty, right = value)
      }
    Outer(both = valueAndZero(DescribeByRow(1, describe(row)(keys))))
  }

  /**
    * Generate [Inner] that describe and compare two rows with the same key
    *
    * @param left           Row from the left Dataset
    * @param right          Row from the right Dataset
    * @param keys           Column's names of both Datasets that are considered as keys
    * @return               Inner information about comparison between left and right
    */
  def inner(left: Row, right: Row)(keys: Set[String]): Inner = {

    val schema = left.asInstanceOf[GenericRowWithSchema].schema

    val byNames: Map[String, Delta] =
      schema.fieldNames
        .filterNot(keys)
        .map(name => name -> Delta(left.getAs[Any](name), right.getAs[Any](name)))
        .toMap

    val isEqual            = byNames.forall(_._2.nEqual == 1)
    val nDiff: Set[String] = if (isEqual) Set.empty else byNames.filter(_._2.nNotEqual > 0).keys.toSet

    if (nDiff.isEmpty) {
      Inner(1, 0, Map.empty, DescribeByRow(1, byNames.mapValues(x => x.describe.left).map(x => x)))
    } else {
      Inner(0, 1, Map(nDiff -> DeltaByRow(1, byNames)), emptyDescribeByRow)
    }
  }

  private val emptyInner: Inner                 = MonoidGen.empty[Inner]
  private val emptyOuter: Outer                 = MonoidGen.empty[Outer]
  private val emptyDescribeByRow: DescribeByRow = MonoidGen.empty[DescribeByRow]

  /**
    * Create a [ParkaResult] from two rows using the same key on both Datasets
    * If a key is only available on one side then we perform an [Outer] analysis instead of an [Inner] one
    * Normally, the key should be unique and the Iterable[Row] should not contains more than one element
    *
    * @param left           Row from the left Dataset for a particular set of key
    * @param right          Row from the right Dataset for a particular set of key
    * @param keys           Column's names of both Datasets that are considered as keys
    * @return               ParkaResult containing outer or inner information for a particular set of key
    */
  def result(left: Iterable[Row], right: Iterable[Row])(keys: Set[String]): ParkaResult =
    (left, right) match {
      //Only  Right
      case (l, r) if l.isEmpty && r.nonEmpty => ParkaResult(emptyInner, outer(r.head, Right)(keys))
      //Only Left
      case (l, r) if l.nonEmpty && r.isEmpty => ParkaResult(emptyInner, outer(l.head, Left)(keys))
      //Inner
      case (l, r) if l.nonEmpty && r.nonEmpty => ParkaResult(inner(l.head, r.head)(keys), emptyOuter)
    }

  def combine(left: ParkaResult, right: ParkaResult): ParkaResult = MonoidUtils.parkaResultMonoid.combine(left, right)

  private def compress(res: ParkaResult): ParkaResult =
    res.copy(inner = res.inner.copy(countDeltaByRow = CompressMap.apply(res.inner.countDeltaByRow, 128)))

  /**
    * Entry point of Parka, giving two Datasets we generate one [ParkaAnalysis]
    * [ParkaResult] is the aggregation of result's row
    *
    * @param leftDs         Left Dataset
    * @param rightDs        Right Dataset
    * @param keyNames       Column's names of both Datasets that are considered as keys
    * @return               Delta QA analysis between leftDs and rightDS
    */
  def apply(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis =
    new ParkaRunner().run(leftDs, rightDs)(keyNames: _*)

  def withConfig(nPartition: Int = -1)(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis =
    new ParkaRunner(configuration = ParkaRunnerConfiguration(forcedPartioner = Some(new HashPartitioner(nPartition))))
      .run(leftDs, rightDs)(keyNames: _*)

  /*
    val df1 = ???
    val df2= ???
    Parka.withConfig(nPartition = 500)(df1, df2)("key1", "key2")
   */

  case class ParkaRunnerConfiguration(forcedPartioner: Option[Partitioner])

  class ParkaRunner(configuration: ParkaRunnerConfiguration = ParkaRunnerConfiguration(None)) {
    def run(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis = {

      assert(keyNames.nonEmpty, "you must have at least one key")
      SchemaComparator.assert(leftDs.schema, rightDs.schema)

      val keyValue: Row => String = r => keyNames.map(r.getAs[Any]).mkString(keyValueSeparator)

      val keys = keyNames.toSet

      val leftAndRight: RDD[(String, (Iterable[Row], Iterable[Row]))] = {

        val leftRDD  = leftDs.toDF.rdd.keyBy(keyValue)
        val rightRDD = rightDs.toDF.rdd.keyBy(keyValue)

        configuration.forcedPartioner.fold(leftRDD.cogroup(rightRDD))(p => leftRDD.cogroup(rightRDD, p))
      }

      val res: ParkaResult = leftAndRight
        .map({
          case (k, (left, right)) => result(left, right)(keys)
        })
        .reduce(combine)

      ParkaAnalysis(datasetInfo = Both(leftDs, rightDs).map(datasetInfo), result = compress(res))
    }

  }

  /**
    * Generate a [ParkaAnalysis] from two CSV file
    *
    * @param leftPath       left Dataset's file path
    * @param rightPath      Right Dataset's file path
    * @param sep            Separator for the dataset (default is ";")
    * @param keyNames       Primary key(s)
    * @param spark          The SparkSession of the application
    * @return               A Parka Analysis
    */
  def fromCSV(leftPath: String, rightPath: String, sep: String = ";")(
    keyNames: String*
  )(implicit spark: SparkSession): ParkaAnalysis = {
    def csvToDf(path: String): sql.DataFrame =
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", sep)
        .load(path)

    val leftDf  = csvToDf(leftPath)
    val rightDf = csvToDf(rightPath)

    Parka(leftDf, rightDf)(keyNames: _*)
  }

  def datasetInfo(ds: Dataset[_]): DatasetInfo = DatasetInfo(Nil, 0L)

  sealed trait Side
  case object Left extends Side
  case object Right extends Side
}
