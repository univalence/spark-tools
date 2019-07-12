package io.univalence.parka

import cats.kernel.Monoid
import com.twitter.algebird.{Moments, QTree}
import io.univalence.parka.Delta.DeltaLong
import io.univalence.parka.Describe.{DescribeLong, DescribeString}
import io.univalence.parka.MonoidGen._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Dataset, Row}


case class DatasetInfo(source: Seq[String], nStage: Long)

case class Both[+T](left: T, right: T) {
  def fold[U](f: (T, T) => U): U = f(left, right)

  def map[U](f: T => U): Both[U] = Both(f(left), f(right))
}

case class Outer(countRow: Both[Long], byColumn: Map[String, Both[Describe]])

sealed trait Describe extends Serializable

object Describe {

  case class DescribeString(sumLength: Long)

  /** TODO : remplacer Describe Long par [[com.twitter.algebird.Moments]]
    * Voir pour mettre en place un Q-Tree pour avoir les quartiles
    */
  case class DescribeLong(qtree: Option[QTree[Unit]]) extends Describe

  object DescribeLong {
    def apply(long: Long): DescribeLong = DescribeLong(Some(QTree.value(long)))
  }

  case class DescribeBoolean(nTrue: Long, nFalse: Long)

  implicit val describeMono: Monoid[Describe] = MonoidGen.gen[DescribeLong].asInstanceOf[Monoid[Describe]]
}

case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDiffByRow: Map[Int, Long],
                 byColumn: Map[String, Delta])

//case class ByColumn[T](columnName:String, value:T)

sealed trait Delta extends Serializable {
  def nEqual: Long
  def nNotEqual: Long
  def describe: Both[Describe]
}

object Delta {
  case class DeltaString(nEqual: Long, nNotEqual: Long, describe: Both[DescribeString], error: Double)

  case class DeltaLong(nEqual: Long, nNotEqual: Long, describe: Both[DescribeLong], error: Moments) extends Delta

  implicit val deltaMonoid: Monoid[Delta] = MonoidGen.gen[DeltaLong].asInstanceOf[Monoid[Delta]]

}

case class ParkaResult(inner: Inner, outer: Outer)

case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result: ParkaResult)

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

  def inner(left: Row, right: Row)(keys: Set[String]): Inner = {

    val schema = left.asInstanceOf[GenericRowWithSchema].schema

    val byNames: Map[String, Delta] = schema.fieldNames
      .filterNot(keys)
      .map(name => {
        val delta: Delta = (left.getAs[Any](name), right.getAs[Any](name)) match {
          case (x: Long, y: Long) =>
            if (x == y) {
              val describe = DescribeLong(x)
              DeltaLong(1, 0, Both(describe, describe), Moments(0))
            } else {
              val diff = x - y
              DeltaLong(0, 1, Both(DescribeLong(x), DescribeLong(y)), Moments(diff))
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

  def result(left: Iterable[Row], right: Iterable[Row])(keys: Set[String]): ParkaResult =
    (left, right) match {
      //Only  Right
      case (l, r) if l.isEmpty && r.nonEmpty => ParkaResult(emptyInner, outer(r.head, Right)(keys))
      //Only Left
      case (l, r) if l.nonEmpty && r.isEmpty => ParkaResult(emptyInner, outer(l.head, Left)(keys))
      //Inner
      case (l, r) if l.nonEmpty && r.nonEmpty => ParkaResult(inner(l.head, r.head)(keys), emptyOuter)
    }

  private val monoidParkaResult = {
    MonoidGen.gen[ParkaResult]
  }

  def combine(left: ParkaResult, right: ParkaResult): ParkaResult =
    monoidParkaResult.combine(left, right)

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
