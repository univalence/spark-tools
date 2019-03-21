package io.univalence.typedpath

import org.scalatest.FunSuite

import scala.util.Try

class PathSpec extends FunSuite {

  //TODO @Harrison fix it!
  ignore("createPath") {

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
  ignore("follow up") {

    assert(Path.create("").get == Root)
    assert(Path.create("abc") == Field("abc",Root))
    assert(Path.create("abc.def/").get == Array(Field("def",Field("abc", Root).get).get))


    /*

   {:abc {:def 1}}   abc.def

    {:abc {:def [{:ghi 1} {:ghi 2}]}}  abc.def/ghi
     */


  }


}
