package io.univalence.parka

import com.twitter.algebird.QTree

case class Histogram(negatives: Option[QTree[Unit]], countZero: Long, positives: Option[QTree[Unit]]) {
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
    } else if (portion <= p2) {
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

  case class Bin(pos: Double, count: Long)

  def bin(n: Int): Seq[Bin] = {
    require(n >= 2, "at least 2 bins")

    val step = (max - min) / (n - 1)

    (0 until n).map { i =>
      val lower = min + step * i - (step / 2)
      val upper = lower + step

      Bin((lower + upper) / 2, approxCountBetween(lower, upper).toLong)
    }
  }

  def fixedBin(n: Int): Seq[Bin] = {
    /*
        Create a list of number with a proportionnal distribution giving a count and size
     */
    def distribute(total: Int, size: Int): Seq[Int] = size match {
      case 0 => Seq.empty
      case 1 => Seq(total)
      case _ if total % size == 0 => Seq.fill(size)(total / size)
      case _ if Math.abs(total) < size => {
        val left  = Seq.fill(Math.abs(total))(if (total > 0) 1 else -1)
        val right = Seq.fill(size - total)(0)
        left ++ right
      }
      case _ => {
        val border = size - (Math.abs(total) % size)
        val value  = total / size
        val left   = Seq.fill(border)(value)
        val right  = Seq.fill(size - border)(value + (if (total > 0) 1 else -1))
        left ++ right
      }
    }

    val bins            = bin(n)
    val bins_sum        = bins.map(_.count).sum
    val diff            = bins_sum - count
    val distributedDiff = distribute(diff.toInt, n)
    bins.zip(distributedDiff).map { case (bin, d) => bin.copy(count = bin.count - d) }
  }

}

object Histogram {
  def empty: Histogram = Histogram(negatives = None, countZero = 0, positives = None)

  def value(x: Long): Histogram =
    if (x == 0)
      Histogram(None, 1, None)
    else if (x < (Long.MinValue + 2))
      value(Long.MinValue + 2)
    else if (x > Long.MaxValue - 2)
      value(Long.MaxValue - 2)
    else if (x < 0)
      Histogram(negatives = Some(QTree.value(-x)), countZero = 0, positives = None)
    else
      Histogram(negatives = None, countZero = 0, positives = Some(QTree.value(x)))

  def value(x: Double): Histogram =
    if (x == 0)
      Histogram(None, 1, None)
    else if (x < 0)
      Histogram(negatives = Some(QTree.value(-x)), countZero = 0, positives = None)
    else
      Histogram(negatives = None, countZero = 0, positives = Some(QTree.value(x)))

}
