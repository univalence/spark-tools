package io.univalence.parka

import cats.kernel.Monoid
import com.twitter.algebird.{ Moments, QTree }
import io.univalence.parka.Delta.DeltaLong
import io.univalence.parka.Describe.{DescribeLong, DescribeString}
import io.univalence.parka.Histogram.LongHisto
import io.univalence.parka.MonoidGen._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Dataset, Row}

case class Both[+T](left: T, right: T) {
  def fold[U](f: (T, T) => U): U = f(left, right)

  def map[U](f: T => U): Both[U] = Both(f(left), f(right))
}

case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result: ParkaResult)
case class DatasetInfo(source: Seq[String], nStage: Long)
case class ParkaResult(inner: Inner, outer: Outer)

/**
  * Inner contains information about rows with similar keys
  *
  * @param countRowEqual          The number of equal rows (with the same values for each columns which are not keys)
  * @param countRowNotEqual       The number of unequal rows
  * @param countDiffByRow         Map with number of differences between two rows as a key and for each them how many times they occured between the two Datasets
  * @param byColumn               Map with column name as a key and for each of them two Describe, one for each row's Dataset - only for inner row
  */
case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDiffByRow: Map[Int, Long],
                 byColumn: Map[String, Delta])

/**
  * Outer contains information about rows that are only present in one of the two datasets
  *
  * @param countRow               The number of additional rows for each Datasets
  * @param byColumn               Map with column name as a key and for each of them two Describe, one for each row's Dataset - only for outer row
  */
case class Outer(countRow: Both[Long], byColumn: Map[String, Both[Describe]])

sealed trait Describe extends Serializable

object Describe {

  case class DescribeString(sumLength: Long)

  /** TODO : remplacer Describe Long par [[com.twitter.algebird.Moments]]
    * Voir pour mettre en place un Q-Tree pour avoir les quartiles
    */
  case class DescribeLong(qtree: LongHisto) extends Describe // Pas sûre qu'un QTree[Unit] fonctionne ici => même sûre que ça ne fonctionne pas

  object DescribeLong {
    def apply(long: Long): DescribeLong = DescribeLong(LongHisto.value(long))
  }

  case class DescribeBoolean(nTrue: Long, nFalse: Long)

  implicit val describeMono: Monoid[Describe] = MonoidGen.gen[DescribeLong].asInstanceOf[Monoid[Describe]]
}

sealed trait Delta extends Serializable {
  def nEqual: Long
  def nNotEqual: Long
  def describe: Both[Describe]
}

object Delta {
  case class DeltaString(nEqual: Long, nNotEqual: Long, describe: Both[DescribeString], error: Double)

  case class DeltaLong(nEqual: Long, nNotEqual: Long, describe: Both[DescribeLong], error: LongHisto)
    extends Delta

  implicit val deltaMonoid: Monoid[Delta] = MonoidGen.gen[DeltaLong].asInstanceOf[Monoid[Delta]]

}

object Parka {

  private val keyValueSeparator = "§"

  def describe(row: Row)(keys: Set[String]): Map[String, Describe] = {
    val fields = row.asInstanceOf[GenericRowWithSchema].schema.fieldNames

    fields
      .filterNot(keys)
      .map(name => {
        val describe = row.getAs[Any](name) match {
          case l: Long => DescribeLong(l)
        }

        name -> describe
      })
      .toMap
  }

  /**
    * @param row            Row from the one of the two Dataset
    * @param side           Right if the row come from the right Dataset otherwise Left
    * @param keys           Column's names of both Datasets that are considered as keys
    *
    * @return               Outer information from one particular row
    */
  def outer(row: Row, side: Side)(keys: Set[String]): Outer =
    Outer(
      countRow = side match {
        case Right => Both(left = 0, right = 1)
        case Left  => Both(left = 1, right = 0)
      },
      byColumn = describe(row)(keys)
        .mapValues(
          d =>
            side match {
              case Right => Both(left = emptyDescribe, right = d)
              case Left  => Both(left = d, right             = emptyDescribe)
          }
        )
        .map(identity) // oH No https://www.youtube.com/watch?v=P-3GOo_nWoc
    )

  /**
    * @param left           Row from the left Dataset
    * @param right          Row from the right Dataset
    * @param keys           Column's names of both Datasets that are considered as keys
    *
    * @return               Inner information about comparison between left and right
    */
  def inner(left: Row, right: Row)(keys: Set[String]): Inner = {

    val schema = left.asInstanceOf[GenericRowWithSchema].schema

    val byNames: Map[String, Delta] =
      schema.fieldNames
        .filterNot(keys)
        .map(name => {
          val delta: Delta = (left.getAs[Any](name), right.getAs[Any](name)) match {
            case (x: Long, y: Long) =>
              if (x == y) {
                val describe = DescribeLong(x)
                DeltaLong(1, 0, Both(describe, describe), LongHisto.value(0))
              } else {
                val diff = x - y // Diff can't be negatif if QTree so "x - y" nop sorry
                DeltaLong(0, 1, Both(DescribeLong(x), DescribeLong(y)), LongHisto.value(diff))
              }
          }
          name -> delta
        })
        .toMap

    val isEqual = byNames.forall(_._2.nEqual == 1)
    val nDiff   = if (isEqual) 0 else byNames.count(_._2.nNotEqual == 1)
    Inner(if (isEqual) 1 else 0, if (isEqual) 0 else 1, Map(nDiff -> 1), byNames)
  }

  private val emptyInner: Inner        = MonoidGen.empty[Inner]
  private val emptyOuter: Outer        = MonoidGen.empty[Outer]
  private val emptyDescribe: Describe  = MonoidGen.empty[Describe]
  private val emptyResult: ParkaResult = MonoidGen.empty[ParkaResult]

  /**
    * @param left           Row from the left Dataset for a particular key
    * @param right          Row from the right Dataset for a particular key
    * @param keys           Column's names of both Datasets that are considered as keys
    *
    * @return               ParkaResult containing outer or inner information for a key
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

  private val monoidParkaResult = MonoidGen.gen[ParkaResult]

  def combine(left: ParkaResult, right: ParkaResult): ParkaResult = monoidParkaResult.combine(left, right)

  /**
    * Entry point of Parka
    *
    * @param leftDs         Left Dataset
    * @param rightDs        Right Dataset
    * @param keyNames       Column's names of both Datasets that are considered as keys
    *
    * @return               Delta QA analysis between leftDs and rightDS
    */
  def apply(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis = {
    assert(keyNames.nonEmpty, "you must have at least one key")
    assert(leftDs.schema == rightDs.schema, "schemas are not equal : " + leftDs.schema + " != " + rightDs.schema)

    val schema = leftDs.schema

    val keyValue: Row => String = r => keyNames.map(r.getAs[Any]).mkString(keyValueSeparator)

    val keys = keyNames.toSet

    val leftAndRight: RDD[(String, (Iterable[Row], Iterable[Row]))] =
      leftDs.toDF.rdd.keyBy(keyValue).cogroup(rightDs.toDF.rdd.keyBy(keyValue))

    val res = leftAndRight
      .map({
        case (k, (left, right)) => result(left, right)(keys)
      })
      .reduce(combine)

    ParkaAnalysis(datasetInfo = Both(leftDs, rightDs).map(datasetInfo), result = res)
  }

  def datasetInfo(ds: Dataset[_]): DatasetInfo = DatasetInfo(Nil, 0L)

  sealed trait Side
  case object Left extends Side
  case object Right extends Side

}
