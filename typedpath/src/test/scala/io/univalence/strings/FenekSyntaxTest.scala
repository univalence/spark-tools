package io.univalence.strings

object FenekSyntaxTest {

  trait StrucField

  implicit class StringOps(val name: String) {
    def <<-(path: Key): StrucField = ???
  }
  implicit class PathOps(val path: Key) {
    def as(name: String): StrucField = ???
  }

  case class struct(field: StrucField*) {
    def addField(field: StrucField*): struct = ???

    def |+(field: StrucField): struct = ???
  }

  val x: struct = struct(
    key"source.source.source.source" as "target1",
    "target0"  <<- key"source.source.source.source",
    "target42" <<- key"source.source.source.source"
  )

  val y: struct = x addField (
    "target2"  <<- key"source",
    "target43" <<- key"source"
  )

  y addField "target" <<- key"source"

}
