package io.univalence.typedpath

import scala.language.experimental.macros
import scala.reflect.macros.whitebox
import scala.util.matching.Regex
import scala.util.{ Failure, Success, Try }

object PathMacro {

  //interleave(Seq(poteau, poteau, poteau), Seq(cloture,cloture)) == Seq(poteau, cloture, poteau, cloture, poteau)
  def interleave[A, B](xa: Seq[A], xb: Seq[B]): Seq[Either[A, B]] = {
    def go(xa: Seq[A], xb: Seq[B], accc: Vector[Either[A, B]]): Vector[Either[A, B]] =
      (xa, xb) match {
        case (Seq(), _)                         => accc
        case (Seq(x, _ @_*), Seq())             => accc :+ Left(x)
        case (Seq(a, as @ _*), Seq(b, bs @ _*)) => go(as, bs, accc :+ Left(a) :+ Right(b))
      }
    go(xa, xb, Vector.empty)
  }

  def pathMacro(c: whitebox.Context)(args: c.Expr[Path]*): c.Expr[NonEmptyPath] = {
    import c.universe._
    import Path.Token

    def lit(s: String): c.Expr[String] = c.Expr[String](Literal(Constant(s)))

    val strings: List[String] = {
      //pattern matching pour récupérer les chaines de du string contexte
      //val q"$x($y(...$rawParts))" = c.prefix.tree
      val Apply(_, List(Apply(_, rawParts))) = c.prefix.tree
      rawParts.map({
        case Literal(Constant(y: String)) => y
      })
    }

    val allParts: Seq[Either[String, c.Expr[Path]]] = interleave(strings, args)

    def create(string: String, base: c.Expr[Path]): c.Expr[Path] =
      if (string.isEmpty) base
      else {

        val tokens: Seq[Token] = Token.tokenize(string)
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

    if (!allParts.exists({
          case Left(x)  => x.nonEmpty
          case Right(p) => p.actualType <:< typeOf[NonEmptyPath]
        })) {
      c.abort(
        c.enclosingPosition,
        s"path [${allParts.filterNot(_.left.exists(_.isEmpty)).map(_.fold(identity, _.tree)).mkString(" - ")}] can't turn into a NonEmptyPath. Use case object Root instead if you want to target the Root."
      )
    }

    val res = allParts.foldLeft[c.Expr[Path]](reify(Root))({
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

    res.asInstanceOf[c.Expr[NonEmptyPath]]
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

  object Token {
    object Pattern {
      case class StartWithPattern(pattern: String) {
        val regex: Regex = ("^(" + pattern + ")(.*?)$").r
        def unapply(string: String): Option[(String, String)] = string match {
          case regex(m, r) => Some((m, r))
          case _           => None
        }
      }
      case class StartWithChar(char: Char) {
        def unapply(string: String): Option[String] =
          if (string.nonEmpty && string.head == char) Some(string.tail) else None
      }

      val Dot   = StartWithChar('.')
      val Slash = StartWithChar('/')
      val Name  = StartWithPattern("\\w+")
      val Error = StartWithPattern("[^\\w/.]+")
    }

    val tokenOf: String => Option[(Token, String)] = {
      case ""                         => None
      case Pattern.Dot(rest)          => Some((Dot, rest))
      case Pattern.Slash(rest)        => Some((Slash, rest))
      case Pattern.Name(name, rest)   => Some((NamePart(name), rest))
      case Pattern.Error(error, rest) => Some((ErrorToken(error), rest))
      case error                      => Some((ErrorToken(error), ""))
    }

    //probablement dans catz ou scalas
    def unfold[A, S](z: S)(f: S => Option[(A, S)]): Vector[A] = {
      def go(z: S, acc: Vector[A]): Vector[A] =
        f(z) match {
          case None         => acc
          case Some((a, s)) => go(s, acc :+ a)
        }
      go(z, Vector.empty)
    }

    val tokenize: String => Seq[Token] = unfold(_)(tokenOf)

    val stringify: Token => String = {
      case Dot               => "."
      case Slash             => "/"
      case NamePart(name)    => name
      case ErrorToken(error) => error
    }

    def stringify(tokens: Token*): String =
      tokens.map(stringify).mkString
  }

  def highlightErrors(tokens: Token*): String =
    tokens
      .map({
        case ErrorToken(part) => s"[$part]"
        case x                => Token.stringify(x)
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
    def path(args: Path*): NonEmptyPath = macro PathMacro.pathMacro

  }

  def create(string: String): Try[Path] = {
    val tokens = Token.tokenize(string)
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

case class Field(name: String with Field.Name, parent: Path) extends NonEmptyPath

object Field {
  sealed trait Name

  def createName(string: String): Try[String with Name] = {
    val regExp = "^[a-zA-Z_][a-zA-Z0-9_]*$".r
    regExp
      .findPrefixMatchOf(string)
      .fold[Try[String with Name]](Failure(new Exception(s"$string is not a valid field name")))(
        _ => Success(string.asInstanceOf[String with Name])
      )
  }

  def apply(name: String, parent: Path): Try[Field] =
    createName(name).map(new Field(_, parent))
}

case class Array(parent: NonEmptyPath) extends NonEmptyPath
