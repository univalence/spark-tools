package io.univalence.parka

import cats.Monoid
import com.twitter.algebird.QTree
import io.univalence.parka.Histogram.Bin

import scala.util.Try

sealed trait Histogram {
  def count: Long
  def bin(n: Int): Seq[Bin]

  def min: Double
  def max: Double

  def quantileBounds(percentile: Double): (Double, Double)

  def toLargeHistogram: LargeHistogram
}

sealed abstract class SmallHistogram[K] extends Histogram {

  def values: Map[K, Long]
  implicit protected def num: Numeric[K]

  final override def count: Long = values.values.sum

  override def bin(n: Int): Seq[Bin] =
    if (values.isEmpty) Nil
    else {
      val min: Double = num.toDouble(values.keys.min)
      val max: Double = num.toDouble(values.keys.max)

      val step: Double = Try(Math.max((max - min) / (n - 1), 1)).getOrElse(1)

      (0 until n).map(i => {
        val center: Double = min + step * i
        Bin(center,
            values
              .collect({
                case (k, m) if num.toDouble(k) <= center + step / 2 && num.toDouble(k) > center - step / 2 => m
              })
              .sum)
      })
    }

  override def quantileBounds(percentile: Double): (Double, Double) = {
    val xs: Seq[(Double, Long)] = values.toSeq
      .map({
        case (k, v) => (num.toDouble(k), v)
      })
      .sortBy(_._1)

    val n: Int = Math.min((percentile * count).toInt, count - 1).toInt

    val ys: Seq[Double] = xs.flatMap(x => Seq.fill(x._2.toInt)(x._1))

    val v: Double = ys(n)

    (v, v)
  }

  override def min: Double = num.toDouble(values.keys.min)

  override def max: Double = num.toDouble(values.keys.max)

}

case class SmallHistogramL(values: Map[Long, Long]) extends SmallHistogram[Long] {
  override def toLargeHistogram: LargeHistogram = {
    import MonoidGen._

    val monoid: Monoid[LargeHistogram] = gen[LargeHistogram]

    monoid.combineAll(values.map({
      case (k, v) => monoid.combineN(LargeHistogram.value(k), v.toInt)
    }))
  }

  override protected def num: Numeric[Long] = Numeric.LongIsIntegral
}

case class SmallHistogramD(values: Map[Double, Long]) extends SmallHistogram[Double] {
  def toLargeHistogram: LargeHistogram = {
    import MonoidGen._

    val monoid: Monoid[LargeHistogram] = gen[LargeHistogram]

    monoid.combineAll(values.map({
      case (k, v) => monoid.combineN(LargeHistogram.value(k), v.toInt)
    }))
  }

  override protected def num: Numeric[Double] = Numeric.DoubleIsFractional
}

case class LargeHistogram(negatives: Option[QTree[Unit]], countZero: Long, positives: Option[QTree[Unit]])
    extends Histogram {
  lazy val positivesCount: Long = positives.map(_.count).getOrElse(0L)
  lazy val negativesCount: Long = negatives.map(_.count).getOrElse(0L)
  lazy val count: Long          = positivesCount + countZero + negativesCount

  lazy val min: Double =
    foldToSeq(x => -x.quantileBounds(1)._2, x => 0.0, x => x.quantileBounds(0)._1)(Math.min, Double.NegativeInfinity)

  lazy val max: Double =
    foldToSeq(x => -x.quantileBounds(0)._1, x => 0.0, x => x.quantileBounds(1)._2)(Math.max, Double.PositiveInfinity)

  def quantileBounds(percentile: Double): (Double, Double) = {
    require(percentile >= 0.0 && percentile <= 1.0, "The given percentile must be of the form 0 <= p <= 1.0")
    val portion = percentile * count

    val p2: Long = negativesCount + countZero

    if (portion <= negativesCount && negatives.isDefined) {
      val (l, u) = negatives.get.quantileBounds(1 - (portion / negativesCount))
      (-u, -l)
    } else if (portion <= p2 && countZero > 0) {
      (0.0, 0.0)
    } else {
      val portionPos: Double = portion - p2
      positives.get.quantileBounds(portionPos / positives.get.count)
    }
  }

  /*def mean(range: (Double, Double)): Double = (range._1 + range._2) / 2
  def firstQuartile(): Double = mean(quantileBounds(0.25))
  def median(): Double = mean(quantileBounds(0.5))
  def thirdQuartile(): Double = mean(quantileBounds(0.75))*/

  private def foldToSeq[R](negT: QTree[Unit] => R, zeroT: Long => R, posT: QTree[Unit] => R)(combine: (R, R) => R,
                                                                                             empty: R): R =
    Seq(negatives.map(negT), Option(countZero).filter(_ > 0).map(zeroT), positives.map(posT)).flatten
      .reduceOption(combine)
      .getOrElse(empty)

  private def approxCountBetween(lower: Double, upper: Double): Double =
    foldToSeq[Double](
      neg => {
        if (lower < 0.0) {
          val l: Double = Math.max(-upper, 0.0)
          val u: Double = -lower
          val (b1, b2)  = neg.rangeCountBounds(l, u)

          (b1 + b2) / 2.0
        } else 0.0
      },
      x => if (lower <= 0 && 0 <= upper) x.toDouble else 0.0,
      pos => {
        if (upper > 0.0) {
          val l: Double = Math.max(lower, 0.0)
          val (b1, b2)  = pos.rangeCountBounds(l, upper)

          (b1 + b2) / 2.0
        } else 0.0
      }
    )(_ + _, 0)

  private def oldBin(n: Int): Seq[Bin] = {
    require(n >= 2, "at least 2 bins")

    val step = (max - min) / (n - 1)

    (0 until n).map { i =>
      val lower = min + step * i - (step / 2)
      val upper = lower + step

      Bin((lower + upper) / 2, approxCountBetween(lower, upper).toLong)
    }
  }

  def bin(n: Int): Seq[Bin] = {
    /*
        Create a list of number with a proportionnal distribution giving a count and size
     */
    def distribute(total: Int, size: Int): Seq[Int] = size match {
      case 0 => Seq.empty
      case 1 => Seq(total)
      case _ if total % size == 0 => Seq.fill(size)(total / size)
      case _ if Math.abs(total) < size =>
        val left  = Seq.fill(Math.abs(total))(if (total > 0) 1 else -1)
        val right = Seq.fill(size - total)(0)
        left ++ right
      case _ =>
        val border = size - (Math.abs(total) % size)
        val value  = total / size
        val left   = Seq.fill(border)(value)
        val right  = Seq.fill(size - border)(value + (if (total > 0) 1 else -1))
        left ++ right
    }

    val bins            = oldBin(n)
    val bins_sum        = bins.map(_.count).sum
    val diff            = bins_sum - count
    val distributedDiff = distribute(diff.toInt, n)
    bins.zip(distributedDiff).map { case (bin, d) => bin.copy(count = bin.count - d) }
  }

  override def toLargeHistogram: LargeHistogram = this
}

object Histogram {
  case class Bin(pos: Double, count: Long)

  def empty: Histogram = SmallHistogramD(Map.empty)

  def value(x: Long): Histogram   = SmallHistogramL(Map(x          -> 1))
  def value(x: Double): Histogram = SmallHistogramD(Map(x.toDouble -> 1))
}

object LargeHistogram {
  def value(x: Long): LargeHistogram =
    if (x == 0)
      LargeHistogram(None, 1, None)
    else if (x < (Long.MinValue + 2))
      value(Long.MinValue + 2)
    else if (x > Long.MaxValue - 2)
      value(Long.MaxValue - 2)
    else if (x < 0)
      LargeHistogram(negatives = Some(QTree.value(-x)), countZero = 0, positives = None)
    else
      LargeHistogram(negatives = None, countZero = 0, positives = Some(QTree.value(x)))

  def value(x: Double): LargeHistogram =
    if (x == 0)
      LargeHistogram(None, 1, None)
    else if (x < 0)
      LargeHistogram(negatives = Some(QTree.value(-x)), countZero = 0, positives = None)
    else
      LargeHistogram(negatives = None, countZero = 0, positives = Some(QTree.value(x)))
}
