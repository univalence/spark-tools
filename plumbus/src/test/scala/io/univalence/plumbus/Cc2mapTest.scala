package io.univalence.plumbus

import org.scalatest.FunSuite

import scala.util.Try

case class PersonCC(name:  String, age: Int, x: Option[Int])
case class PersonCC2(name: String, age: Int)

class Cc2mapTest extends FunSuite {

  val p1 = PersonCC("a", 1, None)
  val m1: Map[String, Any] = Map("name" -> "a", "age" -> 1)
  val p2 = PersonCC("a", 1, Some(3))
  val m2: Map[String, Any] = Map("name" -> "a", "age" -> 1, "x" -> 3)

  val p1_2 = PersonCC2("a", 1)

  test("testFromMap") {
    assert(cc2map.fromMap[PersonCC](m1) == Try(p1))
    assert(cc2map.fromMap[PersonCC](m2) == Try(p2))
  }

  test("testToMap") {
    assert(cc2map.toMap(p1) == m1)
    assert(cc2map.toMap(p2) == m2)

  }

  test("CC to CC") {
    assert(cc2map.fromMap[PersonCC2](cc2map.toMap(p1)) == Try(p1_2))
  }

}
