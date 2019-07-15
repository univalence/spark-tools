package io.univalence.parka

import cats.kernel.Monoid
import io.univalence.parka.Delta.{ DeltaDouble, DeltaLong, DeltaString }
import io.univalence.parka.Describe.{ DescribeBoolean, DescribeDouble, DescribeLong, DescribeString }
import io.univalence.parka.MonoidGen._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{ Dataset, Row }

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

trait CoProductMonoidHelper[T] {
  type Combined <: T
  def lift(t: T): Combined
}

object CoProductMonoidHelper {
  type Aux[In, Out <: In] = CoProductMonoidHelper[In] {
    type Combined = Out
  }
}

object Describe {

  val empty = DescribeCombine(None, None, None, None)

  implicit val coProductMonoidHelper: CoProductMonoidHelper.Aux[Describe, DescribeCombine] =
    new CoProductMonoidHelper[Describe] {
      override type Combined = DescribeCombine

      override def lift(t: Describe): DescribeCombine = t match {
        case dc: DescribeCombine => dc
        case dl: DescribeLong    => empty.copy(long = Some(dl))
        case dd: DescribeDouble  => empty.copy(double = Some(dd))
        case ds: DescribeString  => empty.copy(string = Some(ds))
        case db: DescribeBoolean => empty.copy(boolean = Some(db))
      }
    }

  case class DescribeLong(value: Histogram) extends Describe
  case class DescribeDouble(value: Histogram) extends Describe
  case class DescribeString(length: Histogram) extends Describe
  case class DescribeBoolean(nTrue: Long, nFalse: Long) extends Describe

  case class DescribeCombine(long: Option[DescribeLong],
                             double: Option[DescribeDouble],
                             string: Option[DescribeString],
                             boolean: Option[DescribeBoolean])
      extends Describe

  def apply(long: Long): DescribeLong          = DescribeLong(Histogram.value(long))
  def apply(long: Double): DescribeDouble      = DescribeDouble(Histogram.value(long))
  def apply(string: String): DescribeString    = DescribeString(Histogram.value(string.length.toLong))
  def apply(boolean: Boolean): DescribeBoolean = if (boolean) DescribeBoolean(1, 0) else DescribeBoolean(0, 1)

}

sealed trait Delta extends Serializable {
  def nEqual: Long
  def nNotEqual: Long
  def describe: Both[Describe]
}

object Delta {

  def apply(b1: Boolean, b2: Boolean): DeltaBoolean = {
    val d1 = Describe(b1)
    if (b1 == b2)
      DeltaBoolean(1, 0, Both(d1, d1))
    else
      DeltaBoolean(0, 1, Both(d1, Describe(b2)))
  }

  def levenshtein(s1: String, s2: String): Int = {
    import scala.math._
    def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }
    for {
      j <- 1 to s2.length
      i <- 1 to s1.length
    } dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else
        minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
    dist(s2.length)(s1.length)
  }

  def stringDiff(str1: String, str2: String): Long =
    //Pour l'instant on va faire ça
    levenshtein(str1, str2).toLong

  case class DeltaLong(nEqual: Long, nNotEqual: Long, describe: Both[DescribeLong], error: Histogram) extends Delta

  case class DeltaDouble(nEqual: Long, nNotEqual: Long, describe: Both[DescribeDouble], error: Histogram) extends Delta

  case class DeltaString(nEqual: Long, nNotEqual: Long, describe: Both[DescribeString], error: Histogram) extends Delta

  case class DeltaBoolean(nEqual: Long, nNotEqual: Long, describe: Both[DescribeBoolean]) extends Delta {
    def tt: Long = (describe.left.nTrue + describe.right.nTrue - nNotEqual) / 2
    def tf: Long = (nNotEqual + describe.left.nTrue - describe.right.nTrue) / 2
    def ff: Long = nEqual - tt
    def ft: Long = nNotEqual - tf
  }

  case class DeltaCombine(long: Option[DeltaLong],
                          double: Option[DeltaDouble],
                          string: Option[DeltaString],
                          boolean: Option[DeltaBoolean])
      extends Delta {

    @transient lazy val seq: Seq[Delta]          = Seq(long, double, string, boolean).flatten
    @transient override lazy val nEqual: Long    = seq.map(_.nEqual).sum
    @transient override lazy val nNotEqual: Long = seq.map(_.nNotEqual).sum
    @transient override lazy val describe: Both[Describe] = {
      val monoid = MonoidUtils.bothDescribeMonoid
      seq.map(_.describe).reduceOption(monoid.combine).getOrElse(monoid.empty)
    }
  }

  val empty: DeltaCombine = DeltaCombine(None, None, None, None)

  implicit val coProductMonoidHelper: CoProductMonoidHelper.Aux[Delta, DeltaCombine] =
    new CoProductMonoidHelper[Delta] {
      override type Combined = DeltaCombine

      override def lift(t: Delta): DeltaCombine =
        t match {
          case dc: DeltaCombine => dc
          case dl: DeltaLong    => empty.copy(long = Some(dl))
          case dl: DeltaDouble  => empty.copy(double = Some(dl))
          case ds: DeltaString  => empty.copy(string = Some(ds))
          case db: DeltaBoolean => empty.copy(boolean = Some(db))
        }
    }
}

object MonoidUtils {
  val describeMonoid: Monoid[Describe]           = MonoidGen.gen[Describe]
  val bothDescribeMonoid: Monoid[Both[Describe]] = MonoidGen.gen
}

object Parka {

  private val keyValueSeparator = "§"

  def describe(row: Row)(keys: Set[String]): Map[String, Describe] = {
    val fields = row.asInstanceOf[GenericRowWithSchema].schema.fieldNames

    fields
      .filterNot(keys)
      .map(name => {
        val describe = row.getAs[Any](name) match {
          case l: Long    => Describe(l)
          case d: Double  => Describe(d)
          case s: String  => Describe(s)
          case b: Boolean => Describe(b)
        }
        name -> describe
      })
      .toMap
  }

  /**
    * @param row            Row from the one of the two Dataset
    * @param side           Right if the row come from the right Dataset otherwise Left
    * @param keys           Column's names of both Datasets that are considered as keys
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
                val describe = Describe(x)
                DeltaLong(1, 0, Both(describe, describe), Histogram.value(0))
              } else {
                val diff = x - y
                DeltaLong(0, 1, Both(Describe(x), Describe(y)), Histogram.value(diff))
              }
            case (x: Double, y: Double) =>
              if (x == y) {
                val describe = Describe(x)
                DeltaDouble(1, 0, Both(describe, describe), Histogram.value(0))
              } else {
                val diff = x - y
                DeltaDouble(0, 1, Both(Describe(x), Describe(y)), Histogram.value(diff))
              }
            case (x: String, y: String) =>
              if (x == y) {
                val describe = Describe(x)
                DeltaString(1, 0, Both(describe, describe), Histogram.value(0))
              } else {
                val diff = Delta.stringDiff(x, y)
                DeltaString(0, 1, Both(Describe(x), Describe(y)), Histogram.value(diff))
              }
            case (x: Boolean, y: Boolean) => Delta.apply(x, y)
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

  private val monoidInner = MonoidGen.gen[Inner]

  private val monoidDescribe = MonoidGen.gen[Describe]
  private val monoidOuter    = MonoidGen.gen[Outer]

  //F***
  // Horry sheet
  private val monoidParkaResult = MonoidGen.gen[ParkaResult]

  def combine(left: ParkaResult, right: ParkaResult): ParkaResult = monoidParkaResult.combine(left, right)

  /**
    * Entry point of Parka
    *
    * @param leftDs         Left Dataset
    * @param rightDs        Right Dataset
    * @param keyNames       Column's names of both Datasets that are considered as keys
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
