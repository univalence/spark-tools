package io.univalence.strings

import org.scalatest.FunSuite

import scala.util.{ Failure, Try }

class KeySpec extends FunSuite {

  import Key._

  test("tokenize") {

    assert(Token.tokenize("abc[].def") == Seq(NamePart("abc"), Brackets, Dot, NamePart("def")))

    assert(
      Token.tokenize("$abc[].def") ==
        Seq(ErrorToken("$"), NamePart("abc"), Brackets, Dot, NamePart("def"))
    )

  }

  test("create error") {
    val f = Key.create("$abc[].(edf").asInstanceOf[Failure[_]]
    assert(f.exception.getMessage.contains("[$]"))
    assert(f.exception.getMessage.contains("[(]"))

  }

  test("interpolation") {
    import Key._

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

    val abc: FieldKey = key"abc"

    assert(abc.name == "abc")
    assert(abc.parent == Root)

    val ghi: FieldKey = key"$abc.ghi"

    assert(ghi.name == "ghi")
    assert(ghi.parent == abc)

    val lol: FieldKey  = key"lol" //
    val comp: FieldKey = key"$abc[].$lol"

    assert(comp.name == "lol")
    assert(comp.parent == ArrayKey(abc))

    val comp2: ArrayKey = key"$comp[]"

    assert(comp2.parent == comp)

    val comp3: ArrayKey = key"$comp2"

    assert(comp3 == comp2)

  }

  //TODO @Harrison fix it!
  test("create Key") {
    assert(
      Key.create("abcd.edfg[][].hijk") ==
        FieldKey("hijk", ArrayKey(ArrayKey(FieldKey("edfg", FieldKey("abcd", Root).get).get)))
    )

    assert(Key.create("abc[][][]") == Try(ArrayKey(ArrayKey(ArrayKey(FieldKey("abc", Root).get)))))
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

    assert(Key.create("123").isFailure)
  }
  test("follow up") {

    assert(Key.create("").get == Root)
    assert(Key.create("abc") == FieldKey("abc", Root))
    assert(Key.create("abc.def[]").get == ArrayKey(FieldKey("def", FieldKey("abc", Root).get).get))

    /*
    {:abc {:def 1}}   abc.def
    {:abc {:def [{:ghi 1} {:ghi 2}]}}  abc.def/ghi
   */

  }

}
