package io.univalence.fenek

import io.univalence.fenek.Expr.{ CaseWhenExpr, CaseWhenExprTyped, CaseWhenExprUnTyped, StructField }
import io.univalence.fenek.Fnk.{ Encoder, LowPriority, TypedExpr }
import io.univalence.typedpath.NonEmptyPath
import org.json4s.JsonAST._

import scala.language.implicitConversions
import scala.util.Try

sealed trait Expr

object Expr extends LowPriority {

  implicit def pathToExpr(path: NonEmptyPath): Expr = {
    import io.univalence.typedpath._

    path match {
      case Field(name, Root)   => Expr.Ops.RootField(name)
      case Field(name, parent) => Expr.Ops.SelectField(name, pathToExpr(parent.asInstanceOf[NonEmptyPath]))
      case Array(_)            => throw new Exception("repetition path not supported in fenek")
    }

  }

  case class Struct(fields: Seq[StructField]) extends Expr

  sealed trait CaseWhenExpr {

    def pairs: Seq[(Expr, Expr)]
    def orElse: Option[Expr]
  }

  object CaseWhenExpr {
    def merge[B](
      caseWhenExprTyped1: CaseWhenExprTyped[B],
      caseWhenExprTyped2: CaseWhenExprTyped[B]
    ): CaseWhenExprTyped[B] =
      CaseWhenExprTyped(
        caseWhenExprTyped1.pairs ++ caseWhenExprTyped2.pairs,
        caseWhenExprTyped1.orElse orElse caseWhenExprTyped2.orElse
      )

    def merge(caseWhenExprTyped1: CaseWhenExpr, caseWhenExprTyped2: CaseWhenExpr): CaseWhenExpr =
      CaseWhenExprUnTyped(
        caseWhenExprTyped1.pairs ++ caseWhenExprTyped2.pairs,
        caseWhenExprTyped1.orElse orElse caseWhenExprTyped2.orElse
      )

    def setDefault[A](caseWhenExprTyped: CaseWhenExprTyped[A], value: TypedExpr[A]): CaseWhenExprTyped[A] =
      caseWhenExprTyped.copy(orElse = Some(value))

    def setDefault(caseWhenExprTyped: CaseWhenExpr, value: Expr): CaseWhenExpr =
      caseWhenExprTyped match {
        case CaseWhenExprUnTyped(pairs, _) =>
          CaseWhenExprUnTyped(pairs, Some(value))
        case CaseWhenExprTyped(pairs, _) =>
          CaseWhenExprUnTyped(pairs, Some(value))
      }

    def add[B](caseWhenExprTyped: CaseWhenExprTyped[B], expr: (Expr, TypedExpr[B])): CaseWhenExprTyped[B] =
      caseWhenExprTyped.copy(pairs = expr +: caseWhenExprTyped.pairs)

    def add(caseWhen: CaseWhenExpr, expr: (Expr, Expr)): CaseWhenExpr =
      caseWhen match {
        case CaseWhenExprUnTyped(pairs, orElse) =>
          CaseWhenExprUnTyped(expr +: pairs, orElse)
        case CaseWhenExprTyped(pairs, orElse) =>
          CaseWhenExprUnTyped(expr +: pairs, orElse)
      }

  }

  case class CaseWhenExprUnTyped(pairs: Seq[(Expr, Expr)], orElse: Option[Expr]) extends CaseWhenExpr

  case class CaseWhenExprTyped[B](pairs: Seq[(Expr, TypedExpr[B])], orElse: Option[TypedExpr[B]]) extends CaseWhenExpr {
    def enc: Encoder[B] = pairs.head._2.enc
    //def untyped:CaseWhenExprUnTyped = CaseWhenExprUnTyped(pairs,orElse)
  }

  case class StructField(name: String, source: Expr)

  object StructField {}

  sealed trait Ops extends Expr

  object Ops {
    case class Remove(source: Expr, toRemove: Seq[Expr]) extends Expr
    case class LastElement(expr: Expr) extends Expr
    case class RootField(name: String) extends Expr
    case class SelectField(field: String, source: Expr) extends Ops
    case class Size(source: Expr) extends Ops
    case class DateDiff(datepart: TypedExpr[String], startdate: Expr, enddate: Expr) extends Ops
    case class Left(characterExpr: Expr, n: TypedExpr[Int]) extends Ops
    case class DateAdd(interval: TypedExpr[String], n: TypedExpr[Int], date: Expr) extends Ops
    case class OrElse(expr: Expr, expr2: Expr) extends Ops
    case class FirstElement(expr: Expr) extends Ops
    case class CaseWhen(source: Expr, ifes: CaseWhenExpr) extends Ops
  }
}

object Fnk {

  implicit def t2ToExp[A: Encoder, B: Encoder](t: (A, B)): CaseWhenExprTyped[B] =
    CaseWhenExprTyped(lit(t._1) -> lit(t._2) :: Nil, None)

  implicit def elseToExp1[B: Encoder](t: (Else.type, B)): CaseWhenExprTyped[B] =
    CaseWhenExprTyped(Nil, Some(t._2))

  implicit def elseToExp2[X](t: (Else.type, TypedExpr[X])): CaseWhenExprTyped[X] =
    CaseWhenExprTyped(Nil, Some(t._2))

  implicit def elseToExp3(t: (Else.type, Expr)): CaseWhenExpr =
    CaseWhenExprUnTyped(Nil, Some(t._2))

  implicit def t2toExp2[A: Encoder](t: (A, Expr)): CaseWhenExpr =
    CaseWhenExprUnTyped(lit(t._1) -> t._2 :: Nil, None)

  implicit class t2Ops[A: Encoder, B: Encoder](t: (A, B)) {
    val expr: (TypedExpr[A], TypedExpr[B]) = (lit(t._1), lit(t._2))

    def |(caseWhenExprTyped: CaseWhenExprTyped[B]): CaseWhenExprTyped[B] =
      CaseWhenExpr.add(caseWhenExprTyped, expr)
    def |(caseWhenExpr: CaseWhenExpr): CaseWhenExpr =
      CaseWhenExpr.add(caseWhenExpr, expr)
  }

  implicit class t2Ops2[A: Encoder](t: (A, Expr)) {
    val expr: (TypedExpr[A], Expr) = (lit(t._1), t._2)

    def |(caseWhenExpr: CaseWhenExpr): CaseWhenExpr =
      CaseWhenExpr.add(caseWhenExpr, expr)
  }

  implicit class t2Ops3[A: Encoder](t: (Else.type, A)) {
    def |(caseWhenExprTyped: CaseWhenExprTyped[A]): CaseWhenExprTyped[A] =
      CaseWhenExpr.setDefault(caseWhenExprTyped, lit(t._2))
    def |(caseWhenExpr: CaseWhenExpr): CaseWhenExpr =
      CaseWhenExpr.setDefault(caseWhenExpr, lit(t._2))
  }

  implicit class caseWhenOps[B](caseWhenExprTyped1: CaseWhenExprTyped[B]) {
    def |(caseWhenExprTyped2: CaseWhenExprTyped[B]): CaseWhenExprTyped[B] =
      CaseWhenExpr.merge(caseWhenExprTyped1, caseWhenExprTyped2)
    def |(caseWhenExpr: CaseWhenExpr): CaseWhenExpr =
      CaseWhenExpr.merge(caseWhenExprTyped1, caseWhenExpr)
  }

  implicit class caseWhenUnTypedOps[B](caseWhenExpr1: CaseWhenExpr) {
    def |(caseWhenExpr2: CaseWhenExpr): CaseWhenExpr =
      CaseWhenExpr.merge(caseWhenExpr1, caseWhenExpr2)
  }

  trait LowPriority {
    implicit def antiArrowAssocExpr1[A: Encoder](t: (A, Expr)): (TypedExpr[A], Expr) = (lit(t._1), t._2)

    implicit def antiArrowAssocExpr2[A: Encoder, B](t: (A, TypedExpr[B])): (TypedExpr[A], TypedExpr[B]) =
      (lit(t._1), t._2)

    implicit def antiArrowAssocExpr3[A: Encoder, B: Encoder](t: (A, B)): (TypedExpr[A], TypedExpr[B]) =
      (lit(t._1), lit(t._2))

    implicit def toLit[T: Encoder](t: T): TypedExpr[T] = lit(t)

    implicit class ExprOps(expr: Expr) {

      import Expr._

      object > {
        def select(field: String): Expr = Ops.SelectField(field, expr)
      }

      def firstElement: Expr = Ops.FirstElement(expr)

      def lastElement: Expr = Ops.LastElement(expr)

      def remove(values: Expr*): Expr = Ops.Remove(expr, values)

      def size: Expr = Ops.Size(expr)

      def as[T: Encoder]: TypedExpr[T] =
        TypedExpr.TypeCasted(expr, implicitly[Encoder[T]])

      def orElse(expr2: Expr): Expr = Ops.OrElse(expr, expr2)

      def #>[T: Encoder](f: PartialFunction[JValue, T]): TypedExpr[T] =
        TypedExpr.JsonMap(expr, f, implicitly[Encoder[T]])

      def caseWhen(when: CaseWhenExpr, whens: CaseWhenExpr*): Expr = Ops.CaseWhen(expr, whens.foldLeft(when)(_ | _))

      /*def caseWhen[X](when: CaseWhenExprTyped[X], whens:CaseWhenExprTyped[X]*): TypedExpr[X] =
        TypedExpr.CaseWhenTyped(expr, whens.foldLeft(when)(_ | _))*/

      def dateAdd(interval: TypedExpr[String], n: TypedExpr[Int]): Expr =
        Ops.DateAdd(interval, n, expr)

      def left(n: TypedExpr[Int]): Expr = Ops.Left(expr, n)

      def datediff(datepart: TypedExpr[String], enddate: Expr): Expr =
        Ops.DateDiff(datepart, expr, enddate)

      def isEmpty: TypedExpr[Boolean] = TypedExpr.IsEmpty(expr)
    }
  }

  object Null extends Expr

  object Else

  object struct {
    @deprecated
    def build(call: (String, Expr)*): Expr.Struct = Expr.Struct(call.map((Expr.StructField.apply _).tupled))

    def apply(structField: StructField*): Expr.Struct = Expr.Struct(structField)
  }

  sealed trait Encoder[T]

  object Encoder {
    type SimpleEncoder[T] = Encoder[T]

    implicit case object Str extends SimpleEncoder[String]
    implicit case object Int extends SimpleEncoder[Int]
    implicit case object Bool extends SimpleEncoder[Boolean]
    implicit case object BigDecimal extends SimpleEncoder[BigDecimal]
    implicit case object Double extends SimpleEncoder[Double]
    //implicit def opt[T: Encoder]: Encoder[Option[T]] = ???

  }

  object interval {
    def day: TypedExpr[String] = lit("day")
  }

  sealed abstract class TypedExpr[A] extends Expr {

    import TypedExpr._
    def enc: Encoder[A]

    def orElse(typedExpr2: TypedExpr[A])(implicit encoder: Encoder[A]): TypedExpr[A] =
      TypedOrElse(this, typedExpr2, encoder)

    import TypedExpr._
    def |>[B](f: A => B)(implicit enc: Encoder[B]): TypedExpr[B] =
      Map1[A, B](this, f, enc)

    trait Map2Builder[B] {
      def |>[C: Encoder](f: (A, B) => C): TypedExpr[C]
      def <*>[C: Encoder](typedExpr: TypedExpr[C]): Map3Builder[C]
      trait Map3Builder[C] {
        def |>[D: Encoder](f: (A, B, C) => D): TypedExpr[D]
      }
    }

    def <*>[B](typedExpr: TypedExpr[B]): Map2Builder[B] = {
      val t = this
      new Map2Builder[B] {
        override def |>[C: Encoder](f: (A, B) => C): TypedExpr[C] =
          TypedExpr.Map2(t, typedExpr, f, implicitly[Encoder[C]])

        override def <*>[C: Encoder](typedExpr2: TypedExpr[C]): Map3Builder[C] =
          new Map3Builder[C] {
            override def |>[D: Encoder](f: (A, B, C) => D): TypedExpr[D] =
              Map3[A, B, C, D](t, typedExpr, typedExpr2, f, implicitly[Encoder[D]])
          }
      }
    }

  }

  object TypedExpr {

    case class CaseWhenTyped[B](source: Expr, cases: CaseWhenExprTyped[B]) extends TypedExpr[B] {
      override def enc: Encoder[B] = cases.enc
    }

    case class TypedOrElse[T](value: TypedExpr[T], value1: TypedExpr[T], enc: Encoder[T]) extends TypedExpr[T]

    case class TypeCasted[Scala](source: Expr, enc: Encoder[Scala]) extends TypedExpr[Scala]

    case class JsonMap[O](source: Expr, f: JValue => O, enc: Encoder[O]) extends TypedExpr[O]

    case class Map1[S, O](source: TypedExpr[S], f: S => O, enc: Encoder[O]) extends TypedExpr[O] {
      def tryApply(a: Any): Try[O] = Try(f(a.asInstanceOf[S]))
    }

    case class Map2[A, B, C](first: TypedExpr[A], second: TypedExpr[B], f: (A, B) => C, enc: Encoder[C])
        extends TypedExpr[C] {
      def tryApply(a: Any, b: Any): Try[C] =
        Try(f(a.asInstanceOf[A], b.asInstanceOf[B]))
    }

    case class Map3[A, B, C, D](
      first: TypedExpr[A],
      second: TypedExpr[B],
      third: TypedExpr[C],
      f: (A, B, C) => D,
      enc: Encoder[D]
    ) extends TypedExpr[D] {
      def tryApply(a: Any, b: Any, c: Any): Try[D] =
        Try(f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C]))
    }

    case class Lit[T](value: T, enc: Encoder[T]) extends TypedExpr[T]
    case class IsEmpty(expr: Expr) extends TypedExpr[Boolean] {
      override def enc: Fnk.Encoder[Boolean] = Encoder.Bool
    }
  }

  def lit[T: Encoder](t: T): TypedExpr[T] =
    TypedExpr.Lit(t, implicitly[Encoder[T]])

  object > {
    def apply(fieldName: String): Expr = Expr.Ops.RootField(fieldName)
  }

}

sealed trait GenericExpr {
  import GenericExpr.{ Named, OneOrMore }
  def expr: Named[Expr]
  def sources: Seq[Named[OneOrMore[GenericExpr]]]
  def strs: Seq[Named[String]]
  def values: Seq[Named[Any]]
}

object GenericExpr {

  type OneOrMore[T] = ::[T]

  def OneOrMore[T](t: T, ts: T*): OneOrMore[T] = ::(t, ts.toList)

  case class Named[+T](name: String, value: T)

  def apply(sourceExpr: Expr): GenericExpr = {
    val named = toNamedSeq(sourceExpr)

    new GenericExpr {
      override lazy val expr: Named[Expr] =
        Named(sourceExpr.getClass.getName, sourceExpr)

      override lazy val sources: Seq[Named[OneOrMore[GenericExpr]]] = {
        val res = named.collect({
          case Named(name, expr: Expr) =>
            Named(name, OneOrMore(apply(expr)))
          // a Field could be a expression as well
          case Named(name, StructField(fname, source)) =>
            Named(name + "." + fname, OneOrMore(apply(source)))
          case Named(name, (expr1: Expr, expr2: Expr)) =>
            Named(name, OneOrMore(apply(expr1), apply(expr2)))
        })

        res
      }

      override lazy val strs: Seq[Named[String]] = {
        named.collect({ case Named(name, s: String) => Named(name, s) })
      }
      override lazy val values: Seq[Named[Any]] = {
        expr.value match {
          case TypedExpr.Lit(value, _) => List(Named("value", value))
          case _                       => Nil
        }
      }
    }

  }

  private def toNamedSeq(entity: AnyRef): Seq[Named[Any]] =
    entity.getClass.getDeclaredFields.flatMap(field => {
      field.setAccessible(true)

      field.get(entity) match {
        case Seq() | None | Nil => Nil
        case Some(v)            => List(Named(field.getName, v))
        case xs: Seq[_]         => xs.map(x => Named(field.getName, x))
        case x                  => List(Named(field.getName, x))
      }
    })

}

object Source {

  case class Path(parts: Vector[String])

  def getSources(expr: Expr): Vector[Path] = {

    def loop(genericExpr: GenericExpr, suffix: Vector[String] = Vector.empty): Vector[Path] =
      genericExpr.expr.value match {
        case Expr.Ops.SelectField(name, source) =>
          loop(GenericExpr(source), name +: suffix)
        case Expr.Ops.RootField(name) => Vector(Path(name +: suffix))
        case _ =>
          for {
            sourceline <- genericExpr.sources.toVector
            source     <- sourceline.value
            x          <- loop(source)
          } yield x

      }

    loop(GenericExpr(expr))
  }
}
