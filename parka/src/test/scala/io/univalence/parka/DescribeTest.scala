package io.univalence.parka

import org.scalatest.FunSuiteLike

class DescribeTest extends FunSuiteLike {

  test("Describe enums") {
    val result = Describe("FR")

    assert(result.count == 1)
    assert(result.enums("value").estimate("FR") == 1)
  }

}
