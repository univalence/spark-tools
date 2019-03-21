package io.univalence.typedpath

import eu.timepit.refined._
import shapeless.Witness
import shapeless.tag.@@

import scala.util.{ Failure, Success, Try }

sealed trait Path

object Path {
  type Name = string.MatchesRegex[Witness.`"[a-zA-Z_][a-zA-Z0-9_]*"`.T]

  def createName(string: String): Either[String, String @@ Name] = refineT[Name](string)

  def create(string: String): Try[Path] =
    if (string.isEmpty) {
      Try(Root)
    } else {
      val dotIndex   = string.lastIndexOf('.')
      val slashIndex = string.lastIndexOf('/')

      if (dotIndex == -1 && slashIndex == -1) {
        Field(string, Root)
      } else if (dotIndex > -1 && (dotIndex > slashIndex || slashIndex == -1)) {
        val (xs, x) = string.splitAt(dotIndex)
        val suffix: String = x.tail
        val prefix: String = xs

        for {
          parent <- create(prefix)
          field <- Field(suffix, parent)
        } yield field
      } else {
        val (xs, x) = string.splitAt(slashIndex)
        val suffix  = x.tail
        val prefix  = xs

        val parentPath: Try[Array] =
          create(prefix) flatMap {
            case value: NonEmptyPath => Try(Array(value))
            case value => Failure(new Exception(s"$value non NonEmptyPath"))
          }

        if (suffix == "") {
          parentPath
        } else {
          parentPath.flatMap(Field(suffix, _))
        }
      }
    }
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
