package io.univalence.parka

import cats.kernel.Monoid
import io.univalence.parka.MonoidGen._
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
  * Inner contains information about rows with similar keys between the left and right Datasets
  *
  * @param countRowEqual          The number of equal rows (with the same values for each columns which are not keys)
  * @param countRowNotEqual       The number of unequal rows
  * @param countDeltaByRow        Map for each column that has at least one column that differs with Seq
  *                               of columns that are different as key and a DeltaByRow as value
  * @param equalRows              DescribeByRow that contains Describe for each column of similar rows
  */
case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDeltaByRow: Map[Set[String], DeltaByRow],
                 equalRows: DescribeByRow) {
  @transient lazy val byColumn: Map[String, Delta] =
    Monoid.combineAll(countDeltaByRow.map(_._2.byColumn).toSeq :+ equalRows.byColumn.mapValues(d => {
      Delta(d.count, 0, Both(d, d), Describe.empty)
    }))

}

/**
  * Outer contains information about rows with no similar keys between the left and right Datasets
  *
  * @param both          DescribeByRow for the left and the right Datasets
  */
case class Outer(both: Both[DescribeByRow])

/**
  * Inner side - When there is a difference between the left and right Datasets according to a set of column
  *
  * If we have a similar key for both Datasets and there are value in two columns named 'a' and 'b' that are different from both Datasets
  * Then we obtain a new DeltaByRow for Seq(a, b)
  *
  * @param count                  Number of occurence for a particular set of column
  * @param byColumn               For each keys, the Delta structure that represent the difference between left and right
  */
case class DeltaByRow(count: Long, byColumn: Map[String, Delta])

/**
  * Inner side - When there is no difference between the left and right Datasets
  * Outer side - Every row has a DescribeByRow since there are no similar keys which means no differences
  *
  * @param count                  The number of similar row (similar to countRowEqual for the inner part)
  * @param byColumn               Map with column name as a key and for each of them a Describe
  */
case class DescribeByRow(count: Long, byColumn: Map[String, Describe])

/**
  * Store a large amount of information to describe a particular value
  *
  * @param count          Number of Describe
  * @param histograms     Map of Histogram for each kind of value such as Date, String, Double and so on
  *                       Normally because a column as a type you shouldn't have more than one Histogram per column
  * @param counts         Map of special counts depending of the row type for example, if the row accept nulls then
  *                       there is a count for them stored here with the key "nNull"
  */
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
      case f: Float       => histo("value", f)
      case l: Long        => histo("value", l)
      case i: Int         => histo("value", i)
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
      case (i1: Int, i2: Int)       => Describe(i1 - i2)
      case (d1: Double, d2: Double) => Describe(d1 - d2)
      case (f1: Float, f2: Float)   => Describe(f1 - f2)
      case (s1: String, s2: String) =>
        Describe.histo("levenshtein", levenshtein_generified(s1.toCharArray, s2.toCharArray).toLong)
      case (b1: Boolean, b2: Boolean) =>
        val key: String = (if (b1) "t" else "f") + (if (b2) "t" else "f")
        Describe.count(key, 1)
      case (d1: Date, d2: Date)               => Describe(d1.getTime - d2.getTime)
      case (t1: Timestamp, t2: Timestamp)     => Describe(t1.getTime - t2.getTime)
      case (b1: Array[Byte], b2: Array[Byte]) => Describe.histo("levenstein", levenshtein_generified(b1, b2).toLong)
      case _                                  => ???
    }

  def apply(x: Any, y: Any): Delta =
    if (x == y) Delta(1, 0, Both(Describe(x), Describe(y)), Describe.empty)
    else Delta(0, 1, Both(Describe(x), Describe(y)), error(x, y))
}
