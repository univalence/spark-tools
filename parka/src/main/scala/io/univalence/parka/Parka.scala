package io.univalence.parka

import cats.kernel.Monoid
import io.univalence.parka.MonoidGen._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{ Dataset, Row }
import java.sql.{ Date, Timestamp }

import io.univalence.parka.Delta.DeltaBoolean

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
                 countDiffByRow: Map[Seq[String], Long],
                 byColumn: Map[String, Delta])

/**
  * Outer contains information about rows that are only present in one of the two datasets
  *
  * @param countRow               The number of additional rows for each Datasets
  * @param byColumn               Map with column name as a key and for each of them two Describe, one for each row's Dataset - only for outer row
  */
case class Outer(countRow: Both[Long], byColumn: Both[Map[String, Describe]])

case class Describe(count: Long, histograms: Map[String, Histogram], counts: Map[String, Long])

object Describe {

  val empty: Describe    = MonoidUtils.describeMonoid.empty
  val oneValue: Describe = empty.copy(count = 1)

  final def histo(name: String, l: Long): Describe =
    oneValue.copy(histograms = Map(name -> Histogram.value(l)))
  final def histo(name: String, d: Double): Describe =
    oneValue.copy(histograms = Map(name -> Histogram.value(d)))
  final def count(name: String, value: Long): Describe = oneValue.copy(counts = Map(name -> value))

  def apply(a: Any): Describe =
    a match {
      case null           => count("nNull", 1)
      case true           => count("nTrue", 1)
      case false          => count("nFalse", 1)
      case s: String      => histo("length", s.length.toLong)
      case d: Double      => histo("value", d)
      case l: Long        => histo("value", l)
      case ts: Timestamp  => histo("timestamp", ts.getTime)
      case d: Date        => histo("date", d.getTime)
      case b: Array[Byte] => histo("length", b.length.toLong)
    }
}

case class Delta(nEqual: Long, nNotEqual: Long, describe: Both[Describe], error: Describe) {
  def asBoolean: Option[DeltaBoolean] =
    if (describe.left.counts.isDefinedAt("nTrue") || describe.left.counts.isDefinedAt("nFalse"))
      Option(new DeltaBoolean {
        override val ft: Long = error.counts.getOrElse("ft", 0)
        override val tf: Long = error.counts.getOrElse("tf", 0)
        private val t_ : Long = describe.left.counts.getOrElse("nTrue", 0)
        override val tt: Long = t_ - tf
        override val ff: Long = nEqual - tt
      })
    else None
}

object Delta {

  trait DeltaBoolean {
    def ft: Long
    def tf: Long
    def tt: Long
    def ff: Long

  }

  def levenshtein_generified[T](a1: Array[T], a2: Array[T]): Int = {
    import scala.math._
    def minimum(i1: Int, i2: Int, i3: Int): Int = min(min(i1, i2), i3)
    val dist: Array[Array[Int]] = Array.tabulate(a2.length + 1, a1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }
    for {
      j <- 1 to a2.length
      i <- 1 to a1.length
    } {
      dist(j)(i) = if (a2(j - 1) == a1(i - 1)) {
        dist(j - 1)(i - 1)
      } else {
        minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
      }
    }
    dist(a2.length)(a1.length)
  }

  def error(x: Any, y: Any): Describe =
    (x, y) match {
      case (null, _)                => Describe.count("rightToNull", 1)
      case (_, null)                => Describe.count("leftToNull", 1)
      case (l1: Long, l2: Long)     => Describe(l1 - l2)
      case (d1: Double, d2: Double) => Describe(d1 - d2)
      case (s1: String, s2: String) =>
        Describe.histo("levenshtein", levenshtein_generified(s1.toCharArray, s2.toCharArray).toLong)
      case (b1: Boolean, b2: Boolean) =>
        val key: String = (if (b1) "t" else "f") + (if (b2) "t" else "f")
        Describe.count(key, 1)
      case (d1: Date, d2: Date)               => Describe(d1.getTime - d2.getTime)
      case (t1: Timestamp, t2: Timestamp)     => Describe(t1.getTime - t2.getTime)
      case (b1: Array[Byte], b2: Array[Byte]) => Describe.histo("levenstein", levenshtein_generified(b1, b2).toLong)
    }

  def apply(x: Any, y: Any): Delta =
    if (x == y) Delta(1, 0, Both(Describe(x), Describe(y)), Describe.empty)
    else Delta(0, 1, Both(Describe(x), Describe(y)), error(x, y))
}

object MonoidUtils {
  val describeMonoid: Monoid[Describe]           = MonoidGen.gen[Describe]
  val bothDescribeMonoid: Monoid[Both[Describe]] = MonoidGen.gen

  val parkaResultMonoid: Monoid[ParkaResult] = MonoidGen.gen[ParkaResult]

}

object Parka {

  private val keyValueSeparator = "§"

  def describe(row: Row)(keys: Set[String]): Map[String, Describe] = {
    val fields = row.asInstanceOf[GenericRowWithSchema].schema.fieldNames

    fields
      .filterNot(keys)
      .map(name => name -> Describe(row.getAs[Any](name)))
      .toMap
  }

  /**
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
    Outer(
      countRow = valueAndZero(1),
      byColumn = valueAndZero(describe(row)(keys))
    )
  }

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
        .map(name => name -> Delta(left.getAs[Any](name), right.getAs[Any](name)))
        .toMap

    val isEqual            = byNames.forall(_._2.nEqual == 1)
    val nDiff: Seq[String] = if (isEqual) Nil else byNames.filter(_._2.nNotEqual > 0).keys.toSeq.sorted
    Inner(if (isEqual) 1 else 0, if (isEqual) 0 else 1, if (nDiff.isEmpty) Map.empty else Map(nDiff -> 1), byNames)
  }

  private val emptyInner: Inner = MonoidGen.empty[Inner]
  private val emptyOuter: Outer = MonoidGen.empty[Outer]

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

  def combine(left: ParkaResult, right: ParkaResult): ParkaResult = MonoidUtils.parkaResultMonoid.combine(left, right)

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
