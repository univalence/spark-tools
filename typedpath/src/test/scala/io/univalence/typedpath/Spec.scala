package io.univalence.typedpath

import org.scalatest.FunSuite

import scala.util.Try

class PathSpec extends FunSuite {

  test("createPath") {

    assert(
      Path.create("abcd.edfg//hijk") ==
        Field("hijk", Array(Array(Field("edfg", Field("abcd", Root).get).get)))
    )

    assert(Path.create("abc///") == Try(Array(Array(Array(Field("abc", Root).get)))))
    /*

    //TO TEST

    ""
  "abc"
  "abc/"
  "abc.def/"
  "abc.def/ghi"
  "abc.def//"
   */

  }

}
