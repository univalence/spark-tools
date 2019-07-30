package io.univalence.strings

import io.univalence.strings.Index.{ ArrayIndex, FieldIndex }

import scala.language.experimental.macros
import scala.reflect.macros.whitebox
import scala.util.matching.Regex
import scala.util.{ Failure, Success, Try }

object KeyMacro {

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

  def keyMacro(c: whitebox.Context)(args: c.Expr[KeyOrRoot]*): c.Expr[Key] = {
    import c.universe._

    def lit(s: String): c.Expr[String] = c.Expr[String](Literal(Constant(s)))

    val strings: List[String] = {
      //pattern matching pour récupérer les chaines de du string contexte
      //val q"$x($y(...$rawParts))" = c.prefix.tree
      val Apply(_, List(Apply(_, rawParts))) = c.prefix.tree
      rawParts.map({
        case Literal(Constant(y: String)) => y
      })
    }

    val allParts: Seq[Either[String, c.Expr[KeyOrRoot]]] = interleave(strings, args)

    def create(string: String, base: c.Expr[KeyOrRoot]): c.Expr[KeyOrRoot] =
      if (string.isEmpty) base
      else {

        import Key._
        val tokens: Seq[Token] = Token.tokenize(string)
        if (tokens.exists(_.isInstanceOf[Key.ErrorToken])) {

          val error = Key.highlightErrors(tokens: _*)
          c.abort(c.enclosingPosition, s"invalid key $error")
        } else {

          val validTokens: Seq[Key.ValidToken] = tokens.collect({ case s: Key.ValidToken => s })

          validTokens.foldLeft[c.Expr[KeyOrRoot]](
            base
          )({
            case (parent, Key.NamePart(name)) =>
              //improve checks on name
              reify(FieldKey(lit(name).splice, parent.splice).get)
            case (parent, Key.Dot) => parent
            case (parent, Key.Brackets) if c.typecheck(parent.tree).tpe <:< typeOf[Key] =>
              reify(ArrayKey(parent.splice.asInstanceOf[Key]))

            case (parent, Key.Brackets) =>
              c.abort(
                c.enclosingPosition,
                s"${Option(parent.actualType).getOrElse("")} can't create array from root : ${parent.tree} $string"
              )

          })

        }

      }

    if (!allParts.exists({
          case Left(x)  => x.nonEmpty
          case Right(p) => p.actualType <:< typeOf[Key]
        })) {
      val text = "can't turn into a NonEmptyKey. Use case object Root instead if you want to target the Root."
      c.abort(
        c.enclosingPosition,
        s"key [${allParts.filterNot(_.left.exists(_.isEmpty)).map(_.fold(identity, _.tree)).mkString(" - ")}] $text"
      )
    }

    //.foldLeft[c.Expr[Path]] ...
    val res = allParts.foldLeft[c.Expr[KeyOrRoot]](reify(Root))({
      case (base, Left("")) => base
      case (base, Left(x))  => create(x, base)
      case (base, Right(x)) => {} match {
        case _ if x.actualType == typeOf[ArrayKey] =>
          reify(Key.combineToArray(base.splice, x.splice.asInstanceOf[ArrayKey]))
        case _ if x.actualType == typeOf[FieldKey] =>
          reify(Key.combineToField(base.splice, x.splice.asInstanceOf[FieldKey]))
        case _ =>
          reify(Key.combineToKey(base.splice, x.splice))
      }
    })

    res.asInstanceOf[c.Expr[Key]]
  }
}

sealed trait KeyOrRoot

object Key {

  sealed trait Token

  sealed trait ValidToken extends Token
  case object Dot extends ValidToken // "."
  case object Brackets extends ValidToken
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

      val Dot      = StartWithChar('.')
      val Brackets = StartWithPattern("\\[\\]")
      val Name     = StartWithPattern("\\w+")
      val Error    = StartWithPattern("[^\\w/.]+")
    }

    val tokenOf: String => Option[(Token, String)] = {
      case ""                         => None
      case Pattern.Dot(rest)          => Some((Dot, rest))
      case Pattern.Brackets(_, rest)  => Some((Brackets, rest))
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

    def stringify(tokens: Token*): String =
      if (tokens.isEmpty) ""
      else if (tokens.size == 1) tokens.head match {
        case Dot               => "."
        case Brackets          => "[]"
        case NamePart(name)    => name
        case ErrorToken(error) => error
      } else tokens.map(x => stringify(x)).mkString
  }

  def highlightErrors(tokens: Token*): String =
    tokens
      .map({
        case ErrorToken(part) => s"[$part]"
        case x                => Token.stringify(x)
      })
      .mkString

  def combineToKey(prefix: KeyOrRoot, suffix: KeyOrRoot): KeyOrRoot =
    suffix match {
      case Root        => prefix
      case f: FieldKey => combineToField(prefix, f)
      case a: ArrayKey => combineToArray(prefix, a)
    }

  def combineToArray(prefix: KeyOrRoot, suffix: ArrayKey): ArrayKey =
    ArrayKey(combineToKey(prefix, suffix.parent).asInstanceOf[Key])

  def combineToField(prefix: KeyOrRoot, suffix: FieldKey): FieldKey =
    suffix.copy(parent = combineToKey(prefix, suffix.parent))

  def create(string: String): Try[KeyOrRoot] = {
    val tokens = Token.tokenize(string)
    if (tokens.exists(_.isInstanceOf[ErrorToken])) {

      Failure(
        new Exception(
          s"invalid string ${highlightErrors(tokens: _*)} as a key"
        )
      )

    } else {
      val validToken = tokens.collect({ case v: ValidToken => v })

      validToken.foldLeft[Try[KeyOrRoot]](Try(Root))({
        case (parent, NamePart(name)) => parent.flatMap(FieldKey(name, _))
        case (parent, Dot)            => parent //Meh c'est un peu étrange, mais par construction
        case (parent, Brackets) =>
          parent.flatMap({
            case n: Key => Try(ArrayKey(n))
            case _      => Failure(new Exception(s"cannot create an array at the root $string"))
          })

      })
    }

  }
}

sealed trait IndexOrRoot

case object Root extends KeyOrRoot with IndexOrRoot

sealed trait Index extends IndexOrRoot {

  final def at(name: String with FieldName): FieldIndex = FieldIndex(name, this)
  final def at(idx: Int): ArrayIndex                    = ArrayIndex(idx, this)

  final def firstName: String with FieldName =
    this match {
      case Index.ArrayIndex(_, parent)        => parent.firstName
      case Index.FieldIndex(name, Root)       => name
      case Index.FieldIndex(_, parent: Index) => parent.firstName

    }
}

object Index {

  final def apply(name: FieldName with String): FieldIndex = FieldIndex(name, Root)

  final case class FieldIndex(name: String with FieldName, parent: IndexOrRoot) extends Index {
    override def toString: String = parent.toString + "." + name
  }

  final case class ArrayIndex(idx: Int, parent: Index) extends Index {
    override def toString: String = parent.toString + s"[$idx]"
  }
  //case class ArrayLastElement(parent: Index) extends Index

  def create(index: String): Try[Index] = Try {

    val parts: Array[String] = index.split('.').filter(!_.isEmpty)

    def computeIndex(parent: IndexOrRoot, part: String): IndexOrRoot = {

      val (name, idx) = part.indexOf('[') match {
        case -1 => (part, "")
        case x  => part.splitAt(x)
      }

      val index = FieldIndex(FieldKey.createName(name).get, parent)
      if (idx == null)
        index
      else {
        val Idx = "\\[(\\-?\\d+)\\]".r
        Idx
          .findAllMatchIn(idx)
          .foldLeft[Index](index)((i, s) => ArrayIndex(s.group(1).toInt, i))
      }

    }

    parts.foldLeft[IndexOrRoot](Root)(computeIndex) match {
      case Root     => throw new Exception("incorrect index from string \"" + index + "\"")
      case i: Index => i
    }

  }
}

sealed trait Key extends KeyOrRoot {

  def firstName: String with FieldKey.Name =
    this match {
      case FieldKey(name, Root) => name
      case FieldKey(_, p: Key)  => p.firstName
      case ArrayKey(parent)     => parent.firstName
    }

  def allKeys: List[Key] = {
    def loop(key: Key, stack: List[Key]): List[Key] =
      key match {
        case FieldKey(_, Root)        => key :: stack
        case FieldKey(_, parent: Key) => loop(parent, key :: stack)
        case ArrayKey(parent)         => loop(parent, key :: stack)
      }
    loop(this, Nil)
  }
}

case class FieldKey(name: String with FieldKey.Name, parent: KeyOrRoot) extends Key {
  override def toString: String = parent match {
    case Root        => name
    case f: FieldKey => s"$f.$name"
    case a: ArrayKey => s"$a$name"
  }
}

sealed trait FieldName

object FieldKey {
  type Name = FieldName

  // TODO : @Jon that's ok for you ?
  def createName(string: String): Try[String with Name] = {
    val regExp = "^[a-zA-Z_][a-zA-Z0-9_]*$".r
    regExp
      .findPrefixMatchOf(string)
      .fold[Try[String with Name]](Success(s"""\"${string}\"""".stripMargin.asInstanceOf[String with Name]))(
        _ => Success(string.asInstanceOf[String with Name])
      )
    
      /**.fold[Try[String with Name]](Failure(new Exception(s"$string is not a valid field name")))(
        _ => Success(string.asInstanceOf[String with Name])
      )*/
  }

  def apply(name: String, parent: KeyOrRoot): Try[FieldKey] =
    createName(name).map(new FieldKey(_, parent))
}

case class ArrayKey(parent: Key) extends Key {
  override def toString: String = s"$parent[]"
}
