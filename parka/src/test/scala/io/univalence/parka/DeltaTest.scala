package io.univalence.parka

import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DeltaTest extends FunSuite with ScalaCheckPropertyChecks {

  test("testApply") {
    forAll((xs: Seq[Boolean]) => {
      val (left, right)               = xs.splitAt(xs.size / 2)
      val bb: Seq[(Boolean, Boolean)] = left.zip(right)

      import MonoidGen._
      val monoid = gen[Delta]

      val delta: Delta = monoid.combineAll(bb.map({ case (l, r) => Delta(l, r) }))

      assert(delta.nEqual == bb.count({ case (l, r)    => l == r }))
      assert(delta.nNotEqual == bb.count({ case (l, r) => l != r }))

      def count(left: Boolean, right: Boolean): Long = bb.count(_ == ((left, right)))

      val deltaBoolean = delta.asBoolean.get

      import deltaBoolean._

      assert(ft == count(left = false, right = true))
      assert(tf == count(left = true, right  = false))
      assert(tt == count(left = true, right  = true))
      assert(ff == count(left = false, right = false))

    })

  }

}
