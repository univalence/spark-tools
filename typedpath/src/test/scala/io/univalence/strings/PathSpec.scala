package io.univalence.strings

import org.scalatest.FunSuite

import scala.util.{ Failure, Try }

class PathSpec extends FunSuite {

  import Path._

  test("tokenize") {

    assert(Token.tokenize("abc[].def") == Seq(NamePart("abc"), Brackets, Dot, NamePart("def")))

    assert(
      Token.tokenize("$abc[].def") ==
        Seq(ErrorToken("$"), NamePart("abc"), Brackets, Dot, NamePart("def"))
    )

  }

  test("create error") {
    val f = Path.create("$abc[].(edf").asInstanceOf[Failure[_]]
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
    //check IllTyped
    //val r0 = path"1abc"
    //check Illtyped
    //val r1 = path"$r/"

    //check Illtyped
    //val r2: ArrayPath = path"$r.def/"

    val abc: FieldPath = path"abc"

    assert(abc.name == "abc")
    assert(abc.parent == Root)

    val ghi: FieldPath = path"$abc.ghi"

    assert(ghi.name == "ghi")
    assert(ghi.parent == abc)

    val lol: FieldPath  = path"lol" //
    val comp: FieldPath = path"$abc[].$lol"

    assert(comp.name == "lol")
    assert(comp.parent == ArrayPath(abc))

    val comp2: ArrayPath = path"$comp[]"

    assert(comp2.parent == comp)

    val comp3: ArrayPath = path"$comp2"

    assert(comp3 == comp2)

  }

  //TODO @Harrison fix it!
  test("createPath") {
    assert(
      Path.create("abcd.edfg[][].hijk") ==
        FieldPath("hijk", ArrayPath(ArrayPath(FieldPath("edfg", FieldPath("abcd", Root).get).get)))
    )

    assert(Path.create("abc[][][]") == Try(ArrayPath(ArrayPath(ArrayPath(FieldPath("abc", Root).get)))))
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
    assert(Path.create("abc") == FieldPath("abc", Root))
    assert(Path.create("abc.def[]").get == ArrayPath(FieldPath("def", FieldPath("abc", Root).get).get))

    /*
    {:abc {:def 1}}   abc.def
    {:abc {:def [{:ghi 1} {:ghi 2}]}}  abc.def/ghi
   */

  }

}
