package io.univalence.parka

import org.scalatest.FunSuite

import scala.collection.immutable.BitSet

class KModes2Test extends FunSuite {

  test("testNewKModes2") {

    val kModes2 = KModes2.newKModes2(5, 10)

    assert(kModes2.centers.size == 5)

    assert(kModes2.centers.map(_.max).max <= 10)
  }

  test("fit") {
    val model = KModes2.fit((0 to 2).map(i => 1.0 -> BitSet(i)).toVector, 3, 10)

    assert(model.centers.size == 3)

    (0 to 2).foreach(i => {
      assert(model.centers.contains(BitSet(i)))
    })
  }

}
