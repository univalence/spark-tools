package io.univalence.parka

import cats.kernel.Semigroup
import org.scalatest.FunSuite

class CompressMapTest extends FunSuite {

  test("testApply") {

    val addInt = new Semigroup[Int] {
      override def combine(x: Int, y: Int): Int = x + y
    }

    assert(CompressMap(Map(Set(1, 2) -> 3, Set(2, 3) -> 2), 1)(addInt) == Map(Set(1, 2, 3) -> 5))

    assert(CompressMap(Map(Set(1, 2) -> 3, Set(2, 3) -> 2), 2)(addInt) == Map(Set(1, 2) -> 3, Set(2, 3) -> 2))

    assert(
      CompressMap(Map(
                    Set(1, 2)    -> 3,
                    Set(2, 3)    -> 2,
                    Set(1, 2, 3) -> 0,
                    Set(5, 6)    -> 7,
                    Set(6, 7)    -> 1,
                    Set(5, 6, 7) -> 0
                  ),
                  2)(addInt) == Map(Set(1, 2, 3) -> 5, Set(5, 6, 7) -> 8)
    )

  }

}
