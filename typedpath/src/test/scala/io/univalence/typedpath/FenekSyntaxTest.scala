package io.univalence.typedpath

object FenekSyntaxTest {

  import io.univalence.typedpath.Path._

  case class Field(path: Path, name: String)

  implicit class StringOps(val name: String) {
    def <<-(path: Path): Field = ???
  }
  implicit class PathOps(val path: Path) {
    def as(name: String): Field = ???
  }

  case class struct(field: Field*) {
    def addField(field: Field*): struct = ???

    def |+(field: Field): struct = ???
  }

  val x: struct = struct(path"source.source.source.source" as "target1",
                         "target0"  <<- path"source.source.source.source",
                         "target42" <<- path"source.source.source.source") |+
    "target2"  <<- path"source" |+
    "target43" <<- path"source" |+
    (path"sourceX" as "target3")

  val y: struct = x addField (
    "target2"  <<- path"source",
    "target43" <<- path"source"
  )

  y addField "target" <<- path"source"

}
