package io.univalence.typedpath

import eu.timepit.refined._
import shapeless.Witness
import shapeless.tag.@@

import scala.util.Try

sealed trait Path

object Path {
  type Name = string.MatchesRegex[Witness.`"[a-zA-Z][a-zA-Z0-9_]*"`.T]

  def createName(string: String): Either[String, String @@ Name] = refineT[Name](string)

  def create(string: String): Try[Path] = ???
}

case object Root extends Path

sealed trait NonEmptyPath extends Path

case class Field(name: String @@ Path.Name, parent: Path) extends NonEmptyPath

object Field {
  def apply(name: String, parent: Path): Try[Field] = {
    import scala.util._
    Path.createName(name) match {
      case Left(a)            => Failure(new Exception(a))
      case Right(refinedName) => Success(new Field(refinedName, parent))
    }
  }
}

case class Array(parent: NonEmptyPath) extends NonEmptyPath
