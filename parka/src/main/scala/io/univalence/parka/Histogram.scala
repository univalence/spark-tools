package io.univalence.parka

import com.twitter.algebird.QTree

case class Histogram(neg: Option[QTree[Unit]], countZero: Long, pos: Option[QTree[Unit]]) {
  def count: Long = pos.map(_.count).getOrElse(0L) + countZero + neg.map(_.count).getOrElse(0L)

  def quantileBounds(p: Double): (Double, Double) = {
    require(p >= 0.0 && p <= 1.0, "The given percentile must be of the form 0 <= p <= 1.0")
    val portion = p * count

    val p1: Long = neg.map(_.count).getOrElse(0L)
    val p2: Long = p1 + countZero

    if (portion <= p1 && neg.isDefined) {
      val (l, u) = neg.get.quantileBounds(1 - (portion / p1))
      (-u, -l)
    } else if (portion <= p2) {
      (0.0, 0.0)
    } else {
      val portionPos: Double = portion - p2
      pos.get.quantileBounds(portionPos / pos.get.count)
    }
  }

  /*def mean(range: (Double, Double)): Double = (range._1 + range._2) / 2
  def firstQuartile(): Double = mean(quantileBounds(0.25))
  def median(): Double = mean(quantileBounds(0.5))
  def thirdQuartile(): Double = mean(quantileBounds(0.75))*/

  private def foldToSeq[R](negT: QTree[Unit] => R,
                           countZeroT: Long  => Option[R],
                           posT: QTree[Unit] => R)(combine: (R, R) => R, empty: R): R =
    Seq(neg.map(negT), countZeroT(countZero), pos.map(posT)).flatten.reduceOption(combine).getOrElse(empty)

  lazy val min: Double = foldToSeq(x => -x.quantileBounds(1)._2,
                                   x => if (x > 0) Some(0.0) else None,
                                   x => x.quantileBounds(0)._1)(Math.min, Double.NegativeInfinity)
  lazy val max: Double = foldToSeq(x => -x.quantileBounds(0)._1,
                                   x => if (x > 0) Some(0.0) else None,
                                   x => x.quantileBounds(1)._2)(Math.max, Double.PositiveInfinity)

  private def approxCountBetween(lower: Double, upper: Double): Double =
    foldToSeq[Double](
      neg => {
        if (lower < 0.0) {
          val l: Double = Math.max(-upper, 0)
          val u: Double = -lower
          val (b1, b2)  = neg.rangeCountBounds(l, u)
          (b1 + b2) / 2
        } else 0.0
      },
      x => if (lower <= 0 && 0 <= upper) Some(x) else None,
      pos => {
        if (upper > 0.0) {
          val l: Double = Math.max(lower, 0)
          val (b1, b2)  = pos.rangeCountBounds(l, upper)
          (b1 + b2) / 2
        } else 0.0
      }
    )(_ + _, 0)

  case class Bin(pos: Double, count: Long)

  def bin(n: Int): Seq[Bin] = {
    require(n >= 2, "at least 2 bins")
    val step = (max - min) / (n - 1)
    (0 until n).map(i => {
      val lower = min + step * i - (step / 2)
      val upper = lower + step
      Bin((lower + upper) / 2, approxCountBetween(lower, upper).toLong)
    })
  }

}

object Histogram {
  def empty: Histogram = Histogram(neg = None, countZero = 0, pos = None)

  def value(x: Long): Histogram =
    if (x == 0)
      Histogram(None, 1, None)
    else if (x < (Long.MinValue + 2))
      value(Long.MinValue + 2)
    else if (x > Long.MaxValue - 2)
      value(Long.MaxValue - 2)
    else if (x < 0)
      Histogram(neg = Some(QTree.value(-x)), countZero = 0, pos = None)
    else
      Histogram(neg = None, countZero = 0, pos = Some(QTree.value(x)))

  def value(x: Double): Histogram =
    if (x == 0)
      Histogram(None, 1, None)
    else if (x < 0)
      Histogram(neg = Some(QTree.value(-x)), countZero = 0, pos = None)
    else
      Histogram(neg = None, countZero = 0, pos = Some(QTree.value(x)))

}
