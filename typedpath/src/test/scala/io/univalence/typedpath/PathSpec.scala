package io.univalence.typedpath

import io.univalence.typedpath.Path.{ ErrorToken, NamePart, Slash, Token }
import org.scalatest.FunSuite

import scala.util.{ Failure, Try }

class PathSpec extends FunSuite {

  test("tokenize") {

    assert(Token.tokenize("abc/def") == Seq(NamePart("abc"), Slash, NamePart("def")))

    assert(
      Token.tokenize("$abc/def") ==
        Seq(ErrorToken("$"), NamePart("abc"), Slash, NamePart("def"))
    )

  }

  test("create error") {
    val f = Path.create("$abc/(edf").asInstanceOf[Failure[_]]
    assert(f.exception.getMessage.contains("[$]"))
    assert(f.exception.getMessage.contains("[(]"))

  }

  test("interpolation") {
    import Path._

    val prefix = "abc"

    //TODO : check illtyped
    //val u = Path.create(prefix).map(prefix => path"$prefix.abc")

    //Check IllTyped
    //val x: Root.type = path""

    val r = Root

    //check IllTyped
    //val r1        = path"$r"

    val r2: Array = path"$r.def/"

    val abc: Field = path"abc"

    assert(abc.name == "abc")
    assert(abc.parent == Root)

    val ghi: Field = path"$abc.ghi"

    assert(ghi.name == "ghi")
    assert(ghi.parent == abc)

    val lol: Field  = path"lol" //
    val comp: Field = path"$abc/$lol"

    assert(comp.name == "lol")
    assert(comp.parent == Array(abc))

    val comp2: Array = path"$comp/"

    assert(comp2.parent == comp)

    val comp3: Array = path"$comp2"

    assert(comp3 == comp2)

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
