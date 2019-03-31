package io.univalence.fenek

import io.univalence.fenek.Expr.{ CaseWhenExpr, StructField, UntypedExpr }
import io.univalence.fenek.Fnk.{ Encoder, LowPriority, TypedExpr }
import io.univalence.fenek.generic.GenericExpr
import io.univalence.typedpath.NonEmptyPath
import org.json4s.JsonAST._

import scala.language.implicitConversions
import scala.util.Try

sealed trait Expr[+A]

object Expr extends LowPriority {

  type UntypedExpr = Expr[Any]

  implicit def pathToExpr(path: NonEmptyPath): UntypedExpr = {
    import io.univalence.typedpath._

    path match {
      case Field(name, Root)   => Expr.Ops.RootField(name)
      case Field(name, parent) => Expr.Ops.SelectField(name, pathToExpr(parent.asInstanceOf[NonEmptyPath]))
      case Array(_)            => throw new Exception("repetition path not supported in fenek")
    }

  }

  case class Struct(fields: Seq[StructField]) extends UntypedExpr

  case class CaseWhen[B](source: UntypedExpr, cases: CaseWhenExpr[B]) extends Expr[B]

  case class CaseWhenExpr[+B](pairs: Seq[(UntypedExpr, Expr[B])], orElse: Option[Expr[B]])

  object CaseWhenExpr {
    def merge[B](caseWhenExprTyped1: CaseWhenExpr[B], caseWhenExprTyped2: CaseWhenExpr[B]): CaseWhenExpr[B] =
      CaseWhenExpr(
        caseWhenExprTyped1.pairs ++ caseWhenExprTyped2.pairs,
        caseWhenExprTyped1.orElse orElse caseWhenExprTyped2.orElse
      )

    def setDefault[A](caseWhenExprTyped: CaseWhenExpr[A], value: TypedExpr[A]): CaseWhenExpr[A] =
      caseWhenExprTyped.copy(orElse = Some(value))

    def add[B](caseWhen: CaseWhenExpr[B], expr: (UntypedExpr, Expr[B])): CaseWhenExpr[B] =
      CaseWhenExpr(expr +: caseWhen.pairs, caseWhen.orElse)
  }

  //case class CaseWhenExprUnTyped(pairs: Seq[(UntypedExpr, UntypedExpr)], orElse: Option[UntypedExpr]) extends CaseWhenExpr

  case class StructField(name: String, source: UntypedExpr)

  object StructField {}

  sealed trait Ops extends UntypedExpr

  object Ops {
    case class Remove(source: UntypedExpr, toRemove: Seq[UntypedExpr]) extends UntypedExpr
    case class LastElement(expr: UntypedExpr) extends UntypedExpr
    case class RootField(name: String) extends UntypedExpr
    case class SelectField(field: String, source: UntypedExpr) extends Ops
    case class Size(source: UntypedExpr) extends Ops
    case class DateDiff(datepart: TypedExpr[String], startdate: UntypedExpr, enddate: UntypedExpr) extends Ops
    case class Left(characterExpr: UntypedExpr, n: TypedExpr[Int]) extends Ops
    case class DateAdd(interval: TypedExpr[String], n: TypedExpr[Int], date: UntypedExpr) extends Ops
    case class OrElse(expr: UntypedExpr, expr2: UntypedExpr) extends Ops
    case class FirstElement(expr: UntypedExpr) extends Ops
  }
}

object Fnk {

  implicit def t2ToExp[A: Encoder, B: Encoder](t: (A, B)): CaseWhenExpr[B] =
    CaseWhenExpr(lit(t._1) -> lit(t._2) :: Nil, None)

  implicit def elseToExp1[B: Encoder](t: (Else.type, B)): CaseWhenExpr[B] =
    CaseWhenExpr(Nil, Some(t._2))

  implicit def elseToExp2[X](t: (Else.type, TypedExpr[X])): CaseWhenExpr[X] =
    CaseWhenExpr(Nil, Some(t._2))

  implicit def elseToExp3(t: (Else.type, UntypedExpr)): CaseWhenExpr[Any] =
    CaseWhenExpr(Nil, Some(t._2))

  implicit def t2toExp2[A: Encoder](t: (A, UntypedExpr)): CaseWhenExpr[Any] =
    CaseWhenExpr(lit(t._1) -> t._2 :: Nil, None)

  implicit class t2Ops[A: Encoder, B: Encoder](t: (A, B)) {
    val expr: (TypedExpr[A], TypedExpr[B]) = (lit(t._1), lit(t._2))

    def |[C >: B](caseWhenExprTyped: CaseWhenExpr[C]): CaseWhenExpr[C] =
      CaseWhenExpr.add(caseWhenExprTyped, expr)

  }

  implicit class t2Ops2[A: Encoder](t: (A, UntypedExpr)) {
    val expr: (TypedExpr[A], UntypedExpr) = (lit(t._1), t._2)

    def |(caseWhenExpr: CaseWhenExpr[Any]): CaseWhenExpr[Any] =
      CaseWhenExpr.add(caseWhenExpr, expr)
  }

  implicit class t2Ops3[A: Encoder](t: (Else.type, A)) {
    def |[B >: A](caseWhenExprTyped: CaseWhenExpr[B]): CaseWhenExpr[B] =
      CaseWhenExpr.setDefault(caseWhenExprTyped, lit(t._2))
  }

  implicit class caseWhenOps[B](b: CaseWhenExpr[B]) {
    def |[C >: B](c: CaseWhenExpr[C]): CaseWhenExpr[C] =
      CaseWhenExpr.merge(b, c)
  }

  trait LowPriority {
    implicit def antiArrowAssocExpr1[A: Encoder](t: (A, UntypedExpr)): (TypedExpr[A], UntypedExpr) = (lit(t._1), t._2)

    implicit def antiArrowAssocExpr2[A: Encoder, B](t: (A, TypedExpr[B])): (TypedExpr[A], TypedExpr[B]) =
      (lit(t._1), t._2)

    implicit def antiArrowAssocExpr3[A: Encoder, B: Encoder](t: (A, B)): (TypedExpr[A], TypedExpr[B]) =
      (lit(t._1), lit(t._2))

    implicit def toLit[T: Encoder](t: T): TypedExpr[T] = lit(t)

    implicit class ExprOps(expr: UntypedExpr) {

      import Expr._

      object > {
        def select(field: String): UntypedExpr = Ops.SelectField(field, expr)
      }

      def firstElement: UntypedExpr = Ops.FirstElement(expr)

      def lastElement: UntypedExpr = Ops.LastElement(expr)

      def remove(values: UntypedExpr*): UntypedExpr = Ops.Remove(expr, values)

      def size: UntypedExpr = Ops.Size(expr)

      def as[T: Encoder]: TypedExpr[T] =
        TypedExpr.TypeCasted(expr, implicitly[Encoder[T]])

      def orElse(expr2: UntypedExpr): UntypedExpr = Ops.OrElse(expr, expr2)

      def #>[T: Encoder](f: PartialFunction[JValue, T]): TypedExpr[T] =
        TypedExpr.JsonMap(expr, f, implicitly[Encoder[T]])

      //def caseWhen(when: CaseWhenExpr, whens: CaseWhenExpr*): UntypedExpr = Ops.CaseWhen(expr, whens.foldLeft(when)(_ | _))

      def caseWhen[X](when: CaseWhenExpr[X], whens: CaseWhenExpr[X]*): CaseWhen[X] =
        CaseWhen(expr, whens.foldLeft[CaseWhenExpr[X]](when)(_ | _))

      def dateAdd(interval: TypedExpr[String], n: TypedExpr[Int]): UntypedExpr =
        Ops.DateAdd(interval, n, expr)

      def left(n: TypedExpr[Int]): UntypedExpr = Ops.Left(expr, n)

      def datediff(datepart: TypedExpr[String], enddate: UntypedExpr): UntypedExpr =
        Ops.DateDiff(datepart, expr, enddate)

      def isEmpty: TypedExpr[Boolean] = TypedExpr.IsEmpty(expr)
    }
  }

  object Null extends UntypedExpr

  object Else

  object struct {
    @deprecated("use struct(\"a\" <<- expr) directly", "0.3")
    def build(call: (String, UntypedExpr)*): Expr.Struct = Expr.Struct(call.map((Expr.StructField.apply _).tupled))

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

  sealed abstract class TypedExpr[+A <: Any] extends Expr[A] {

    import TypedExpr._
    //def enc: Encoder[A]

    def orElse[B >: A](typedExpr2: TypedExpr[B])(implicit encoder: Encoder[B]): TypedExpr[B] =
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

    case class TypedOrElse[T](value: TypedExpr[T], value1: TypedExpr[T], enc: Encoder[T]) extends TypedExpr[T]

    case class TypeCasted[A](source: UntypedExpr, enc: Encoder[A]) extends TypedExpr[A]

    case class JsonMap[O](source: UntypedExpr, f: JValue => O, enc: Encoder[O]) extends TypedExpr[O]

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
    case class IsEmpty(expr: UntypedExpr) extends TypedExpr[Boolean]
  }

  def lit[T: Encoder](t: T): TypedExpr[T] =
    TypedExpr.Lit(t, implicitly[Encoder[T]])

  object > {
    def apply(fieldName: String): UntypedExpr = Expr.Ops.RootField(fieldName)
  }

}
