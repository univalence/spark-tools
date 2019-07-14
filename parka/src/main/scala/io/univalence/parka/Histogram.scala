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

  //https://github.com/glamp/bashplotlib
  def asciiDisplayFromDylan(longHisto: Histogram): String = ???

}
