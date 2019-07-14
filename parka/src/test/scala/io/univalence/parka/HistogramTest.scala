package io.univalence.parka

import cats.kernel.Semigroup
import com.twitter.algebird.{ QTree, QTreeSemigroup }
import io.univalence.parka.Histogram.LongHisto
import org.scalatest.prop.{ PropertyChecks, TableDrivenPropertyChecks }
import org.scalatest.{ FunSuite, PropSpec }

class HistogramTest extends FunSuite with PropertyChecks {

  test("work with MinValue and MaxValue") {
    LongHisto.value(Long.MinValue)
    LongHisto.value(Long.MaxValue)
  }

  def invariant(xs: Seq[Long]): Unit =
    if (xs.distinct.size >= 2 && xs.forall(x => (x < Long.MaxValue - 2) && (x > Long.MinValue + 2))) {
      val ys = xs.sorted
      import MonoidGen._
      val histo = MonoidGen.gen[LongHisto].combineAll(xs.view.map(LongHisto.value))
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

}
