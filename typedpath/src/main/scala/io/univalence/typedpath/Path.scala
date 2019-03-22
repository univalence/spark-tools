package io.univalence.typedpath

import eu.timepit.refined._
import shapeless.Witness
import shapeless.tag.@@

import scala.reflect.macros.whitebox
import language.experimental.macros
import scala.util.{ Failure, Success, Try }

object PathMacro {

  class Serialize(val c: whitebox.Context) {
    import c.universe._

    def apply(field: Field): c.Expr[Field] = {
      val n = c.Expr[String](Literal(Constant(field.name)))
      val p = apply(field.parent)
      reify(Field(n.splice, p.splice).get)
    }

    def apply(array: Array): c.Expr[Array] = {
      val p = apply(array.parent)
      reify(Array(p.splice))
    }

    def apply(nonEmptyPath: NonEmptyPath): c.Expr[NonEmptyPath] =
      nonEmptyPath match {
        case a: Array => apply(a)
        case f: Field => apply(f)
      }
    def apply(path: Path): c.Expr[Path] =
      path match {
        case Root => reify(Root)
        case f: Field => apply(f)
        case a: Array => apply(a)
      }

  }

  def pathMacro(c: whitebox.Context)(args: c.Expr[Path]*): c.Expr[Path] = {
    import c.universe._

    //pattern matching pour récupérer les chaines de du string contexte
    val Apply(_, List(Apply(_, rawParts))) = c.prefix.tree
    //val q"$x($y(...$rawParts))" = c.prefix.tree

    val head :: tail: List[String] = rawParts.map({ case Literal(Constant(y: String)) => y })

    val allParts: List[Either[String, c.Expr[Path]]] =
      (Left(head) :: args.map(Right(_)).zip(tail.map(Left(_))).flatMap({ case (a, b) => Seq(a, b) }).toList).reverse

    val serialize = new Serialize(c)

    def lit(s: String): c.Expr[String] = c.Expr[String](Literal(Constant(s)))

    def create(string: String, base: c.Expr[Path]): c.Expr[Path] =
      if (string.isEmpty) base
      else {
        /*
        match {
          case Failure(e) => c.abort(c.enclosingPosition,e.getMessage)
          case Success(path) =>
            reify(Path.combine(fold(xs).splice,serialize(path).splice))
        }*/

        val slashIndex = string.lastIndexOf('/')
        val dotIndex   = string.lastIndexOf('.')
        if (dotIndex == -1 && slashIndex == -1) {
          reify(Field(lit(string).splice, base.splice).get)
        } else if (dotIndex > -1 && (dotIndex > slashIndex || slashIndex == -1)) {

          val (xs, x) = string.splitAt(dotIndex)
          val suffix: String = x.tail
          val prefix: String = xs
          reify(Field(lit(suffix).splice, create(prefix, base).splice).get)
        } else {
          val (xs, x) = string.splitAt(slashIndex)
          val suffix  = x.tail
          val prefix  = xs

          if (suffix == "") {
            reify(Array(create(prefix, base).splice.asInstanceOf[NonEmptyPath]))
          } else {
            reify(Field(lit(suffix).splice, Array(create(prefix, base).splice.asInstanceOf[NonEmptyPath])).get)
          }
        }

      }

    def fold(allParts: List[Either[String, c.Expr[Path]]]): c.Expr[Path] =
      allParts match {
        case Nil            => reify(Root)
        case Left("") :: xs => fold(xs)
        case Left(x) :: xs  => create(x, fold(xs))
        case Right(x) :: xs =>
          reify(Path.combine(fold(xs).splice, x.splice))
      }

    val res = fold(allParts)

    //c.warning(c.enclosingPosition,res.toString())
    res

    //pour les args, récupérer le vrai type en dessus de Path (Path, NonEmpty)

    /*
    c.abort(c.enclosingPosition,"j'ai pas fini")
   */
  }
}

sealed trait Path

object Path {

  def combine(prefix: Path, suffix: Path): Path =
    suffix match {
      case Root => prefix
      case f: Field => combine(prefix, f)
      case a: Array => combine(prefix, a)
    }

  def combine(prefix: Path, suffix: Array): Array =
    Array(combine(prefix, suffix.parent).asInstanceOf[NonEmptyPath])

  def combine(prefix: Path, suffix: Field): Field =
    suffix.copy(parent = combine(prefix, suffix.parent))

  implicit class PathHelper(val sc: StringContext) extends AnyVal {
    def path(args: Path*): Any = macro PathMacro.pathMacro

  }

  type Name = string.MatchesRegex[Witness.`"[a-zA-Z_][a-zA-Z0-9_]*"`.T]

  def createName(string: String): Either[String, String @@ Name] = refineT[Name](string)

  //TODO implementation alternative avec les parseurs combinators
  //TODO implementation alternative avec une PEG grammar
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
