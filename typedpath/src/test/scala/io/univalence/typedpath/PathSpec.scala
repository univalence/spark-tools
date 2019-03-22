package io.univalence.typedpath

import io.univalence.typedpath.Path.{ ErrorToken, NamePart, Slash }
import org.scalatest.FunSuite

import scala.util.Try

class PathSpec extends FunSuite {

  test("tokenize") {

    assert(Path.tokenize("abc/def") == Seq(NamePart("abc"), Slash, NamePart("def")))

    assert(
      Path.tokenize("$abc/def") ==
        Seq(ErrorToken("$"), NamePart("abc"), Slash, NamePart("def"))
    )

  }

  test("interpolation") {
    import Path._

    val prefix = "abc"

    //illtyped
    val u = Path.create(prefix).map(prefix => path"$prefix.abc")

    val x: Root.type = path""

    val y = path"abc"

    assert(y.name == "abc")

    val abc: Field   = path"abc"
    val ghi: Field   = path"$abc.ghi" // >.abc.>.ghi
    val lol: Field   = path"lol" //
    val comp: Field  = path"$abc/$lol"
    val comp2: Array = path"$comp/"
    val comp3: Array = path"$comp2"

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
