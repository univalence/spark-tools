package io.univalence.typedpath

import io.univalence.typedpath.Path.Name

import scala.language.experimental.macros
import scala.reflect.macros.whitebox
import scala.util.{ Failure, Success, Try }

object PathMacro {

  /*
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

  }*/

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
        .toList)

    def lit(s: String): c.Expr[String] = c.Expr[String](Literal(Constant(s)))

    def create(string: String, base: c.Expr[Path]): c.Expr[Path] =
      if (string.isEmpty) base
      else {

        val tokens: Seq[Path.Token] = Path.tokenize(string)
        if (tokens.exists(_.isInstanceOf[Path.ErrorToken])) {

          val error = Path.highlightErrors(tokens: _*)
          c.abort(c.enclosingPosition, s"invalid path $error")
        } else {

          val validTokens: Seq[Path.ValidToken] = tokens.collect({ case s: Path.ValidToken => s })

          validTokens.foldLeft[c.Expr[Path]](
            base
          )({
            case (parent, Path.NamePart(name)) =>
              //improve checks on name
              reify(Field(lit(name).splice, parent.splice).get)
            case (parent, Path.Dot) => parent
            case (parent, Path.Slash) if c.typecheck(parent.tree).tpe <:< typeOf[NonEmptyPath] =>
              reify(Array(parent.splice.asInstanceOf[NonEmptyPath]))

            case (parent, Path.Slash) =>
              c.abort(c.enclosingPosition, s"${parent.actualType} can't create array from root : $parent $string")

          })

        }

      }

    //c.warning(c.enclosingPosition,s"$allParts \n ${head::tail} \n $args")

    allParts.foldLeft[c.Expr[Path]](reify(Root))({
      case (base, Left("")) => base
      case (base, Left(x))  => create(x, base)
      case (base, Right(x)) => {} match {
        case _ if x.actualType == typeOf[Array] =>
          reify(Path.combineToArray(base.splice, x.splice.asInstanceOf[Array]))
        case _ if x.actualType == typeOf[Field] =>
          reify(Path.combineToField(base.splice, x.splice.asInstanceOf[Field]))
        case _ =>
          reify(Path.combineToPath(base.splice, x.splice))
      }
    })

    //c.warning(c.enclosingPosition,res.toString())

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

  def highlightErrors(tokens: Token*): String =
    tokens
      .map({
        case ErrorToken(part) => s"[$part]"
        case x                => stringify(x)
      })
      .mkString

  def combineToPath(prefix: Path, suffix: Path): Path =
    suffix match {
      case Root     => prefix
      case f: Field => combineToField(prefix, f)
      case a: Array => combineToArray(prefix, a)
    }

  def combineToArray(prefix: Path, suffix: Array): Array =
    Array(combineToPath(prefix, suffix.parent).asInstanceOf[NonEmptyPath])

  def combineToField(prefix: Path, suffix: Field): Field =
    suffix.copy(parent = combineToPath(prefix, suffix.parent))

  implicit class PathHelper(val sc: StringContext) extends AnyVal {
    def path(args: Path*): Path = macro PathMacro.pathMacro

  }

  //TODO : remove refine (le moins de dépendances, le mieux)
  sealed trait Name //= string.MatchesRegex[Witness.`""`.T]

  def createName(string: String): Try[String with Name] = {
    val regExp = "^[a-zA-Z_][a-zA-Z0-9_]*$".r
    regExp
      .findPrefixMatchOf(string)
      .fold[Try[String with Name]](Failure(new Exception(s"$string is not a valid field name")))(
        _ => Success(string.asInstanceOf[String with Name])
      )
  }

  def create(string: String): Try[Path] = {
    val tokens = tokenize(string)
    if (tokens.exists(_.isInstanceOf[ErrorToken])) {

      Failure(
        new Exception(
          s"invalid string ${highlightErrors(tokens: _*)} as a path"
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

case class Field(name: String with Path.Name, parent: Path) extends NonEmptyPath

object Field {
  def apply(name: String, parent: Path): Try[Field] =
    Path.createName(name).map(new Field(_, parent))
}

case class Array(parent: NonEmptyPath) extends NonEmptyPath
