package io.univalence.typedpath

import io.univalence.typedpath.Index.{ ArrayIndex, FieldIndex }
import org.scalatest.FunSuite

import scala.util.Try

class IndexTest extends FunSuite {

  //TODO
  ignore("testCreate") {
    assert(Index.create("abc[1].defg") == Try(FieldIndex(name"defg", ArrayIndex(1, FieldIndex(name"abc", Root)))))
  }

}
