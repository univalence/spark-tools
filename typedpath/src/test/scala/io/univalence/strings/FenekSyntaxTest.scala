package io.univalence.strings

object FenekSyntaxTest {

  trait StrucField

  implicit class StringOps(val name: String) {
    def <<-(path: Path): StrucField = ???
  }
  implicit class PathOps(val path: Path) {
    def as(name: String): StrucField = ???
  }

  case class struct(field: StrucField*) {
    def addField(field: StrucField*): struct = ???

    def |+(field: StrucField): struct = ???
  }

  val x: struct = struct(
    path"source.source.source.source" as "target1",
    "target0"  <<- path"source.source.source.source",
    "target42" <<- path"source.source.source.source"
  )

  val y: struct = x addField (
    "target2"  <<- path"source",
    "target43" <<- path"source"
  )

  y addField "target" <<- path"source"

}
