package io.univalence.parka

import cats.Monoid
import io.univalence.parka.MonoidGen._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.FunSuite

import scala.collection.immutable

trait HistogramTest {
  private def assertIn(t: (Double, Double), value: Long): Unit =
    assert(t._1 <= value.toDouble && value.toDouble <= t._2)

  def assertHistoEqual(longHisto: Histogram, value: Long*): Unit =
    longHisto match {
      case smallHistogram: SmallHistogramD =>
        assert(smallHistogram.values == value.groupBy(x => x.toDouble).mapValues(_.size))

      case smallHistogram: SmallHistogramL =>
        assert(smallHistogram.values == value.groupBy(x => x).mapValues(_.size))

      case largeHistogram: LargeHistogram =>
        val sorted = value.sorted

        if (sorted.isEmpty)
          assert(longHisto.count == 0)
        else if (sorted.size == 1)
          assertIn(largeHistogram.quantileBounds(1), value.head)
        else {
          sorted.zipWithIndex.foreach {
            case (v, index) =>
              val d: Double = Math.max(index.toDouble / (sorted.size - 1), 0.00001)
              val lowUp     = largeHistogram.quantileBounds(d)
              //println(s"$index, $d, $lowUp, $v")
              assertIn(lowUp, v)
          }
        }

    }

}

class HistogramTestTest extends FunSuite with ScalaCheckPropertyChecks with HistogramTest {

  val histogramMonoid: Monoid[Histogram] = MonoidGen.histogramMonoid

  test("work with MinValue and MaxValue") {
    Histogram.value(Long.MinValue)
    Histogram.value(Long.MaxValue)
  }

  def invariant(xs: Seq[Long], large: Boolean): Unit =
    if (xs.distinct.size >= 2 && xs.forall(x => (x < Long.MaxValue - 2) && (x > Long.MinValue + 2))) {
      val ys = xs.sorted
      val histo = {

        val histo = histogramMonoid.combineAll(xs.view.map(Histogram.value))
        if (large)
          histo.toLargeHistogram
        else
          histo
      }

      assert(histo.count == ys.length)

      ys.zipWithIndex.foreach({
        case (value, index) =>
          val d      = index.toDouble / (ys.size - 1)
          val (l, u) = histo.quantileBounds(d)

          assert(value >= l)
          assert(value <= u)
      })
    }

  test("vector(0,1), false") {
    invariant(Vector(0, 1), large = false)
  }

  test("vector(2,1), true") {
    invariant(Vector(2, 1), large = true)
  }

  test("Histo Neg") {

    val value   = -1633067063087769L
    val histo_0 = LargeHistogram.value(value)

    assert(histo_0.min == value - 1)
    assert(histo_0.max == value)
    assert(histo_0.quantileBounds(0) == ((value - 1, value)))
    assert(histo_0.quantileBounds(1) == ((value - 1, value)))

    val histo_1 = LargeHistogram.value(0)
    assert(histo_1.min == 0)
    assert(histo_1.max == 0)
    assert(histo_1.quantileBounds(0) == ((0, 0)))
    assert(histo_1.quantileBounds(1) == ((0, 0)))

    val histo_2 = MonoidGen.gen[LargeHistogram].combine(histo_0, histo_1)
    assert(histo_2.min == value - 1)
    assert(histo_2.max == 0)
    assert(histo_2.quantileBounds(0) == ((value - 1, value)))

  }

  test("vector ... true") {
    invariant(Vector(-1633067063087769L, 0L), large = true)
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
    assert((22 to 42).size == 21)
    assert(bins.map(_.count).sum == 21)
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

  test("fix bins => value 100, 1 time") {
    val histo: Histogram = histogramMonoid.combineAll(List(Histogram.value(100)))
    assert(histo.bin(6).map(_.count).sum == histo.count)
  }

  test("fix bins => value 100, 10 times") {
    val histo: Histogram = histogramMonoid.combineAll(List.fill(10)(Histogram.value(100)))
    assert(histo.bin(6).map(_.count).sum == histo.count)
  }

}
