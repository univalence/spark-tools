package io.univalence.fenek

import io.univalence.fenek.Expr.{ StructField, UntypedExpr }
import io.univalence.fenek.Fnk.{ Else, TypedExpr }
import io.univalence.typedpath.Path
import org.json4s.JsonAST._

import scala.language.implicitConversions
import scala.util.Try

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

trait ExprOps[+A] {

  protected def sourceExpr: Expr[A]
  import Expr._

  object > {
    def select(field: String): UntypedExpr = Ops.SelectField(field, sourceExpr)
  }

  def firstElement: UntypedExpr = Ops.FirstElement(sourceExpr)

  def lastElement: UntypedExpr = Ops.LastElement(sourceExpr)

  def remove(values: UntypedExpr*): UntypedExpr = Ops.Remove(sourceExpr, values)

  def size: UntypedExpr = Ops.Size(sourceExpr)

  def as(name: String): StructField = StructField(name, sourceExpr)

  def as[T: Encoder]: TypedExpr[T] =
    TypedExpr.TypeCasted(sourceExpr, implicitly[Encoder[T]])

  def orElse[B >: A](expr2: Expr[B]): Expr[B] = Ops.OrElse(sourceExpr, expr2)

  def #>[T: Encoder](f: PartialFunction[JValue, T]): TypedExpr[T] =
    TypedExpr.JsonMap(sourceExpr, f, implicitly[Encoder[T]])

  def caseWhen[X](when: CaseWhenExpr[X], whens: CaseWhenExpr[X]*): CaseWhen[X] =
    CaseWhen(sourceExpr, whens.foldLeft[CaseWhenExpr[X]](when)(_ orWhen _))

  def dateAdd(interval: TypedExpr[String], n: TypedExpr[Int]): Expr[String] =
    Ops.DateAdd(interval, n, sourceExpr)

  def left(n: TypedExpr[Int]): Expr[String] = Ops.Left(sourceExpr.as[String], n)

  def datediff(datepart: TypedExpr[String], enddate: UntypedExpr): UntypedExpr =
    Ops.DateDiff(datepart, sourceExpr, enddate)

  def isEmpty: TypedExpr[Boolean] = TypedExpr.IsEmpty(sourceExpr)
}

sealed class Expr[+A] extends ExprOps[A] {
  final override protected def sourceExpr: Expr[A] = this
}

object Expr {

  type UntypedExpr = Expr[Any]

  implicit def pathToExpr(path: Path): UntypedExpr = {
    import io.univalence.typedpath._

    path match {
      case Field(name, Root)   => Expr.Ops.RootField(name)
      case Field(name, parent) => Expr.Ops.SelectField(name, pathToExpr(parent.asInstanceOf[Path]))
      case Array(_)            => throw new Exception("repetition path (array) not supported in fenek")
    }

  }

  case class Struct(fields: Seq[StructField]) extends UntypedExpr

  case class CaseWhen[B](source: UntypedExpr, cases: CaseWhenExpr[B]) extends Expr[B]

  case class CaseWhenExpr[+B] protected[fenek] (pairs: Seq[(UntypedExpr, Expr[B])], orElse: Option[Expr[B]]) {

    def orWhen[C >: B](caseWhenExpr: CaseWhenExpr[C]): CaseWhenExpr[C] = CaseWhenExpr.merge(this, caseWhenExpr)
  }

  implicit def lit[T: Encoder](t: T): TypedExpr[T] =
    TypedExpr.Lit(t, implicitly[Encoder[T]])

  object CaseWhenExpr {
    def single[A, B](a: Expr[A], b: Expr[B]): CaseWhenExpr[B] = CaseWhenExpr((a, b) :: Nil, None)

    def apply[B](caseWhenExpr1: CaseWhenExpr[B],
                 caseWhenExpr2: CaseWhenExpr[B],
                 more: CaseWhenExpr[B]*): CaseWhenExpr[B] =
      (caseWhenExpr2 +: more).foldLeft(caseWhenExpr1)(_ orWhen _)

    implicit def t2ToCaseWhenExpr[A, B, C](t: (A, B))(implicit left: ToExpr[A],
                                                      right: ToExpr.Aux[B, C]): CaseWhenExpr[C] =
      CaseWhenExpr.single(left.expr(t._1), right.expr(t._2))

    implicit def elseCaseWhenExpr[B, C](t: (Else.type, B))(implicit toExpr: ToExpr.Aux[B, C]): CaseWhenExpr[C] =
      new CaseWhenExpr(Nil, Some(toExpr.expr(t._2)))

    trait ToExpr[-A] {
      type Out
      def expr(a: A): Expr[Out]
    }

    object ToExpr {
      type Aux[A, B] = ToExpr[A] { type Out = B }

      def instance[A, B](f: A => Expr[B]): ToExpr.Aux[A, B] = new ToExpr[A] {
        type Out = B
        override def expr(a: A): Expr[B] = f(a)
      }

      implicit def encoder[A: Encoder]: Aux[A, A] = instance(a => a)
      implicit def expr[A]: Aux[Expr[A], A]       = instance(a => a)
      implicit val path: Aux[Path, Any]           = instance(a => a)
    }

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
    case class Remove[B](source: Expr[B], toRemove: Seq[Expr[B]]) extends Expr[B]
    case class LastElement(expr: UntypedExpr) extends UntypedExpr
    case class RootField(name: String) extends UntypedExpr
    case class SelectField(field: String, source: UntypedExpr) extends Ops
    case class Size(source: UntypedExpr) extends Ops
    case class DateDiff(datepart: TypedExpr[String], startdate: UntypedExpr, enddate: UntypedExpr) extends Ops
    case class Left(characterExpr: Expr[String], n: TypedExpr[Int]) extends Expr[String]
    case class DateAdd(interval: TypedExpr[String], n: TypedExpr[Int], date: UntypedExpr) extends Expr[String]
    case class OrElse[B](expr: Expr[B], expr2: Expr[B]) extends Expr[B]
    case class FirstElement(expr: UntypedExpr) extends Ops
  }
}

object Fnk {

  object Null extends UntypedExpr

  object Else

  object struct {
    @deprecated("use struct(\"a\" <<- expr) directly", "0.3")
    def build(call: (String, UntypedExpr)*): Expr.Struct = Expr.Struct(call.map((Expr.StructField.apply _).tupled))

    def apply(structField: StructField*): Expr.Struct = Expr.Struct(structField)
  }

  object interval {
    def day: TypedExpr[String] = Expr.lit("day")
  }

  sealed abstract class TypedExpr[+A <: Any] extends Expr[A] {
    //def enc: Encoder[A]

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

}
