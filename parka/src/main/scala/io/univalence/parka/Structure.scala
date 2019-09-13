package io.univalence.parka

import cats.kernel.Monoid
import io.univalence.parka.MonoidGen._
import java.sql.{ Date, Timestamp }

import com.twitter.algebird.{ SketchMap, SketchMapMonoid, SketchMapParams }
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
  * @param enums
  */
case class Describe(count: Long,
                    histograms: Map[String, Histogram],
                    counts: Map[String, Long],
                    enums: Map[String, Enum])

object Describe {

  val empty: Describe    = MonoidUtils.describeMonoid.empty
  val oneValue: Describe = empty.copy(count = 1)

  final def histo(name: String, l: Long): Describe =
    oneValue.copy(histograms = Map(name -> Histogram.value(l)))
  final def histo(name: String, d: Double): Describe =
    oneValue.copy(histograms = Map(name -> Histogram.value(d)))
  final def count(name: String, value: Long): Describe =
    oneValue.copy(counts = Map(name -> value))
  final def enum(name: String, value: String): Describe =
    oneValue.copy(enums = Map(name -> Enum.unit(value)))

  final def enum2(name: String, left: String, right: String): Describe =
    oneValue.copy(enums = Map(name -> Enum.unit(left, right)))

  def apply(a: Any): Describe =
    a match {
      case null           => count("nNull", 1)
      case true           => count("nTrue", 1)
      case false          => count("nFalse", 1)
      case s: String      => enum("value", s)
      case f: Float       => histo("value", f)
      case d: Double      => histo("value", d)
      case i: Int         => histo("value", i)
      case l: Long        => histo("value", l)
      case ts: Timestamp  => histo("timestamp", ts.getTime)
      case d: Date        => histo("date", d.getTime)
      case b: Array[Byte] => histo("length", b.length.toLong)
      case _              => ???
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
      case (null, _) => Describe.count("rightToNull", 1)
      case (_, null) => Describe.count("leftToNull", 1)
      case (b1: Boolean, b2: Boolean) =>
        val key: String = (if (b1) "t" else "f") + (if (b2) "t" else "f")
        Describe.count(key, 1)
      case (s1: String, s2: String) =>
        //Describe.histo("levenshtein", levenshtein_generified(s1.toCharArray, s2.toCharArray).toLong)
        Describe.enum2("from_to", s1, s2)
      case (f1: Float, f2: Float)             => Describe(f1 - f2)
      case (d1: Double, d2: Double)           => Describe(d1 - d2)
      case (i1: Int, i2: Int)                 => Describe(i1 - i2)
      case (l1: Long, l2: Long)               => Describe(l1 - l2)
      case (t1: Timestamp, t2: Timestamp)     => Describe(t1.getTime - t2.getTime)
      case (d1: Date, d2: Date)               => Describe(d1.getTime - d2.getTime)
      case (b1: Array[Byte], b2: Array[Byte]) => Describe.histo("levenshtein", levenshtein_generified(b1, b2).toLong)
      case _                                  => ???
    }

  def apply(x: Any, y: Any): Delta =
    if (x == y) Delta(1, 0, Both(Describe(x), Describe(y)), Describe.empty)
    else Delta(0, 1, Both(Describe(x), Describe(y)), error(x, y))
}

sealed trait EnumKey

case class StringEnumKey(str: String) extends EnumKey{
  override def toString() = str
}

case class BothStringEnumKey(both: Both[String]) extends EnumKey{
  override def toString() = s"${both.left} -> ${both.right}"
}

object BothStringEnumKey {
  def apply(left: String, right: String): BothStringEnumKey = new BothStringEnumKey(Both(left, right))
}

sealed trait Enum {
  def estimate(key: EnumKey): Long
  def estimate(key: String): Long = estimate(StringEnumKey(key))

  def heavyHitters: Map[EnumKey, Long]

  def total: Long

  def add(str: EnumKey): Enum

  def toLargeStringEnum: LargeEnum
}

case class SmallEnum(data: Map[EnumKey, Long]) extends Enum {
  def estimate(key: EnumKey): Long = data(key)

  override def heavyHitters: Map[EnumKey, Long] = data

  override def total: Long = data.values.sum

  override def add(str: EnumKey): Enum = {
    val res = SmallEnum(data.updated(str, data.getOrElse(str, 0L) + 1))
    if (res.data.size > Enum.Sketch.HEAVY_HITTERS_COUNT) res.toLargeStringEnum else res
  }

  def toLargeStringEnum: LargeEnum = LargeEnum(Enum.Sketch.MONOID.create(data.toSeq))
}

case class LargeEnum(sketch: SketchMap[EnumKey, Long]) extends Enum {

  override def estimate(key: EnumKey): Long = Enum.Sketch.MONOID.frequency(sketch, key)

  override def heavyHitters: Map[EnumKey, Long] =
    sketch.heavyHitterKeys.map(s => (s, estimate(s))).toMap

  override def total: Long = sketch.totalValue

  override def add(str: EnumKey): Enum =
    LargeEnum(Enum.Sketch.MONOID.combine(sketch, Enum.Sketch.MONOID.create((str, 1L))))

  override def toLargeStringEnum: LargeEnum = this
}

object Enum {
  def unit(string: String): Enum              = SmallEnum(Map(StringEnumKey(string)          -> 1))
  def unit(left: String, right: String): Enum = SmallEnum(Map(BothStringEnumKey(left, right) -> 1))

  object Sketch {

    val DELTA = 1E-8
    // DELTA: Double = 1.0E-8

    val EPS = 0.001
    // EPS: Double = 0.001

    val SEED = 1
    // SEED: Int = 1

    val HEAVY_HITTERS_COUNT = 256

    val PARAMS: SketchMapParams[EnumKey] = SketchMapParams[EnumKey](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)({

      case StringEnumKey(s)     => s.getBytes
      case BothStringEnumKey(b) => b.map(_.getBytes).fold(_ ++ "$".getBytes ++ _)
    })
    // PARAMS: com.twitter.algebird.SketchMapParams[String] = SketchMapParams(1,2719,19,10)

    val MONOID: SketchMapMonoid[EnumKey, Long] = SketchMap.monoid[EnumKey, Long](PARAMS)

  }
}
