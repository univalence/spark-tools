package io.univalence.parka

import cats.Monoid
import io.univalence.parka.MonoidGen._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.FunSuite

import scala.collection.immutable

trait HistogramTest {
  private def assertIn(t: (Double, Double), value: Long): Unit =
    assert(t._1 <= value.toDouble && value.toDouble <= t._2)

  def assertHistoEqual(longHisto: Histogram, value: Long*): Unit = {
    val sorted = value.sorted

    if (sorted.isEmpty)
      assert(longHisto.count == 0)
    else if (sorted.size == 1)
      assertIn(longHisto.quantileBounds(1), value.head)
    else {
      sorted.zipWithIndex.foreach {
        case (v, index) =>
          val d: Double = Math.max(index.toDouble / (sorted.size - 1), 0.00001)
          val lowUp     = longHisto.quantileBounds(d)
          //println(s"$index, $d, $lowUp, $v")
          assertIn(lowUp, v)
      }
    }
  }

}

class HistogramTestTest extends FunSuite with ScalaCheckPropertyChecks with HistogramTest {

  val histogramMonoid: Monoid[Histogram] = MonoidGen.gen[Histogram]

  test("work with MinValue and MaxValue") {
    Histogram.value(Long.MinValue)
    Histogram.value(Long.MaxValue)
  }

  def invariant(xs: Seq[Long]): Unit =
    if (xs.distinct.size >= 2 && xs.forall(x => (x < Long.MaxValue - 2) && (x > Long.MinValue + 2))) {
      val ys    = xs.sorted
      val histo = histogramMonoid.combineAll(xs.view.map(Histogram.value))

      assert(histo.count == ys.length)

      ys.zipWithIndex.foreach({
        case (value, index) =>
          val d      = index.toDouble / (ys.size - 1)
          val (l, u) = histo.quantileBounds(d)

          assert(value >= l)
          assert(value <= u)
      })
    }

  test("hello") {
    forAll(invariant _)
  }

  test("bins") {
    val histo: Histogram = histogramMonoid.combineAll((22 to 42).map(x => Histogram.value(x.toDouble)))
    val bins             = histo.bin(6)

    assert(bins.size == 6)
    assert(bins.head.pos == histo.min)
    assert(bins.last.pos == histo.max)
    assert(bins.map(_.count).sum == 20)
  }

  test("1 2 3") {
    val values: Seq[Long] = 1L to 3
    assertHistoEqual(histogramMonoid.combineAll(values.map(x => Histogram.value(x))), values: _*)
  }
  test("3 3") {
    val values: Seq[Long] = Seq(3, 3)
    assertHistoEqual(histogramMonoid.combineAll(values.map(x => Histogram.value(x))), values: _*)
  }

  test("test test") {
    forAll((xs: Seq[Long]) => assertHistoEqual(histogramMonoid.combineAll(xs.map(x => Histogram.value(x))), xs: _*))

  }

}
