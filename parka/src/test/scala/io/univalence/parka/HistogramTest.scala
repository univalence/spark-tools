package io.univalence.parka

import io.univalence.parka.MonoidGen._
import org.scalatest.prop.PropertyChecks
import org.scalatest.FunSuite

class HistogramTest extends FunSuite with PropertyChecks {

  test("work with MinValue and MaxValue") {
    Histogram.value(Long.MinValue)
    Histogram.value(Long.MaxValue)
  }

  def invariant(xs: Seq[Long]): Unit =
    if (xs.distinct.size >= 2 && xs.forall(x => (x < Long.MaxValue - 2) && (x > Long.MinValue + 2))) {
      val ys = xs.sorted
      import MonoidGen._
      val histo = MonoidGen.gen[Histogram].combineAll(xs.view.map(Histogram.value))
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

    val histo = (22 to 42).map(x => Histogram.value(x.toDouble)).reduce(gen[Histogram].combine)

    val bins = histo.bin(6)
    assert(bins.size == 6)
    assert(bins.head.pos == histo.min)
    assert(bins.last.pos == histo.max)
    assert(bins.map(_.count).sum == 20)

  }

}
