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
        case Root     => reify(Root)
        case f: Field => apply(f)
        case a: Array => apply(a)
      }

  }

  def pathMacro(c: whitebox.Context)(args: c.Expr[Path]*): c.Expr[Path] = {
    import c.universe._

    //pattern matching pour récupérer les chaines de du string contexte
    val Apply(_, List(Apply(_, rawParts))) = c.prefix.tree
    //val q"$x($y(...$rawParts))" = c.prefix.tree

    val head :: tail: List[String] = rawParts.map({
      case Literal(Constant(y: String)) => y
    })

    val allParts: List[Either[String, c.Expr[Path]]] =
      (Left(head) :: args
        .map(Right(_))
        .zip(tail.map(Left(_)))
        .flatMap({ case (a, b) => Seq(a, b) })
        .toList).reverse

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

          val (xs, x)        = string.splitAt(dotIndex)
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
            reify(
              Field(
                lit(suffix).splice,
                Array(
                  create(prefix, base).splice
                    .asInstanceOf[NonEmptyPath]
                )
              ).get
            )
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

  sealed trait Token

  sealed trait ValidToken extends Token
  case object Dot extends ValidToken // "."
  case object Slash extends ValidToken //  "/"
  case class NamePart(name: String) extends ValidToken // "[a-zA-Z0-9_]+"
  case class ErrorToken(part: String) extends Token

  def tokenize(string: String): Seq[Token] = {
    val validTokenRegExp = "\\.|/|[a-zA-Z0-9_]+".r

    def go(string: String, acc: Seq[Token]): Seq[Token] =
      if (string.isEmpty) acc
      else {
        validTokenRegExp.findFirstMatchIn(string) match {
          case Some(matcher) =>
            val (beginning, rest) = string.splitAt(matcher.end)
            val (error_, matched) = beginning.splitAt(matcher.start)

            val matchedToken: ValidToken = matched match {
              case "."  => Dot
              case "/"  => Slash
              case name => NamePart(name)
            }

            if (error_.isEmpty) go(rest, acc :+ matchedToken)
            else go(rest, acc :+ ErrorToken(error_) :+ matchedToken)

          case None => acc :+ ErrorToken(string)
        }

      }
    go(string, Vector.empty)
  }

  def stringify(token: Token*): String =
    token
    //.view
      .map({
        case Dot           => "."
        case Slash         => "/"
        case ErrorToken(x) => x
        case NamePart(x)   => x
      })
      .mkString

  def combine(prefix: Path, suffix: Path): Path =
    suffix match {
      case Root     => prefix
      case f: Field => combine(prefix, f)
      case a: Array => combine(prefix, a)
    }

  def combine(prefix: Path, suffix: Array): Array =
    Array(combine(prefix, suffix.parent).asInstanceOf[NonEmptyPath])

  def combine(prefix: Path, suffix: Field): Field =
    suffix.copy(parent = combine(prefix, suffix.parent))

  implicit class PathHelper(val sc: StringContext) extends AnyVal {
    def path(args: Path*): Path = macro PathMacro.pathMacro

  }

  //TODO : remove refine (le moins de dépendances, le mieux)
  type Name = string.MatchesRegex[Witness.`"[a-zA-Z_][a-zA-Z0-9_]*"`.T]

  def createName(string: String): Either[String, String @@ Name] =
    refineT[Name](string)

  def create(string: String): Try[Path] = {
    val tokens = tokenize(string)
    if (tokens.exists(_.isInstanceOf[ErrorToken])) {

      val error = tokens.map({
        case ErrorToken(part) => s"[$part]"
        case x                => stringify(x)
      })

      Failure(
        new Exception(
          s"invalid string $error as a path"
        )
      )
    } else {
      val validToken = tokens.collect({ case v: ValidToken => v })

      validToken.foldLeft[Try[Path]](Try(Root))({
        case (parent, NamePart(name)) => parent.flatMap(Field(name, _))
        case (parent, Dot)            => parent //Meh c'est un peu étrange, mais par construction
        case (parent, Slash) =>
          parent.flatMap({
            case n: NonEmptyPath => Try(Array(n))
            case _               => Failure(new Exception(s"cannot create an array at the root $string"))
          })

      })
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
