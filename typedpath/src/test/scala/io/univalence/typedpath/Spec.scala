package io.univalence.typedpath

import org.scalatest.FunSuite

import scala.util.Try

class PathSpec extends FunSuite {

  ignore("interpolation") {
    import Path._

    val prefix = "abc"

    //illtyped
    val u = Path.create(prefix).map(prefix => path"$prefix.<:€")

    val x: Root.type = path""

    val y = path"abc"

    assert(y.name == "abc")

    val abc:      Field = path"abc"
    val ghi:      Field = path"$abc.ghi" // >.abc.>.ghi
    val lol:      Field = path"lol" //
    val compose:  Field = path"$abc/$lol"
    val compose2: Array = path"$compose/"

  }

  //TODO @Harrison fix it!
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

  ["abc" "def" :/ "ghi"]
   */
  }

  test("error") {

    assert(Path.create("123").isFailure)
  }
  test("follow up") {

    assert(Path.create("").get == Root)
    assert(Path.create("abc") == Field("abc", Root))
    assert(Path.create("abc.def/").get == Array(Field("def", Field("abc", Root).get).get))
    /*

   {:abc {:def 1}}   abc.def

    {:abc {:def [{:ghi 1} {:ghi 2}]}}  abc.def/ghi
   */

  }

}
