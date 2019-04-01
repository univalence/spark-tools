package io.univalence.typedpath

object FenekSyntaxTest {

  import io.univalence.typedpath.Path._

  case class Field(path: PathOrRoot, name: String)

  implicit class StringOps(val name: String) {
    def <<-(path: PathOrRoot): Field = ???
  }
  implicit class PathOps(val path: PathOrRoot) {
    def as(name: String): Field = ???
  }

  case class struct(field: Field*) {
    def addField(field: Field*): struct = ???

    def |+(field: Field): struct = ???
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
