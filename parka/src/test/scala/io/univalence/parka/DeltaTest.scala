package io.univalence.parka

import io.univalence.parka.Delta.DeltaBoolean
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

class DeltaTest extends FunSuite with PropertyChecks {

  test("testApply") {

    import MonoidGen._

    forAll((xs: Seq[Boolean]) => {
      val (left, right)               = xs.splitAt(xs.size / 2)
      val bb: Seq[(Boolean, Boolean)] = left.zip(right)

      val monoid = MonoidGen.gen[DeltaBoolean]

      val delta: DeltaBoolean = bb.map((Delta.apply _).tupled).reduceOption(monoid.combine).getOrElse(monoid.empty)

      assert(delta.nEqual == bb.count({ case (l, r)    => l == r }))
      assert(delta.nNotEqual == bb.count({ case (l, r) => l != r }))

      assert(delta.ff == bb.count(_ == ((false, false))), "at ff")
      assert(delta.tf == bb.count(_ == ((true, false))), "at tf")
      assert(delta.ft == bb.count(_ == ((false, true))), "at ft")
      assert(delta.tt == bb.count(_ == ((true, true))), "at tt")
    })

  }

}
