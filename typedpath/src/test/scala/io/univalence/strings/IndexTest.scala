package io.univalence.strings

import io.univalence.strings.Index.{ ArrayIndex, FieldIndex }
import org.scalatest.FunSuite

import scala.util.Try

class IndexTest extends FunSuite {

  test("testCreate index") {
    assert(Index.create("abc[1].defg").get == (Index(name"abc") at 1 at name"defg"))

    assert(Index.create("abc[1][2][3].defg").get == (Index(name"abc") at 1 at 2 at 3 at name"defg"))

    assert(Index.create("abc[-1].defg").get == (Index(name"abc") at -1 at name"defg"))
  }
}
