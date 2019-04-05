package io.univalence.fenek

import io.univalence.fenek.Expr.Ops.{ IsEmpty, JsonMap, Lit, Map1, Map2, Map3, SelectField, TypeCasted }
import io.univalence.fenek.Expr.{ StructField, UntypedExpr }
import io.univalence.typedpath.{ FieldPath, Path }
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

sealed class Expr[+A] {
  final def self: Expr[A] = this

  import Expr._

  def select(path: FieldPath) = SelectField(path, this)

  def firstElement: UntypedExpr = Ops.FirstElement(self)

  def lastElement: UntypedExpr = Ops.LastElement(self)

  def remove(values: UntypedExpr*): UntypedExpr = Ops.Remove(self, values)

  def size: UntypedExpr = Ops.Size(self)

  def as(name: String): StructField = StructField(name, self)

  def as[T: Encoder]: Expr[T] = TypeCasted(self, implicitly[Encoder[T]])

  def orElse[B >: A](expr2: Expr[B]): Expr[B] = Ops.OrElse(self, expr2)

  def #>[T: Encoder](f: PartialFunction[JValue, T]): Expr[T] = JsonMap(self, f, implicitly[Encoder[T]])

  def caseWhen[X](when: CaseWhenExpr[X], whens: CaseWhenExpr[X]*): CaseWhen[X] =
    CaseWhen(self, whens.foldLeft[CaseWhenExpr[X]](when)(_ orWhen _))

  def dateAdd(interval: Expr[String], n: Expr[Int]): Expr[String] =
    Ops.DateAdd(interval, n, self)

  def left(n: Expr[Int]): Expr[String] = Ops.Left(self.as[String], n)

  def datediff(datepart: Expr[String], enddate: UntypedExpr): UntypedExpr =
    Ops.DateDiff(datepart, self, enddate)

  def isEmpty: Expr[Boolean] = IsEmpty(self)

  def |>[B](f: A => B)(implicit enc: Encoder[B]): Expr[B] = Map1[A, B](this, f, enc)

  trait Map2Builder[B] {
    def |>[C: Encoder](f: (A, B) => C): Expr[C]
    def <*>[C: Encoder](typedExpr: Expr[C]): Map3Builder[C]

    trait Map3Builder[C] {
      def |>[D: Encoder](f: (A, B, C) => D): Expr[D]
    }
  }

  def <*>[B](expr2: Expr[B]): Map2Builder[B] = {
    val expr1 = this
    new Map2Builder[B] {
      override def |>[C: Encoder](f: (A, B) => C) = Map2(expr1, expr2, f, implicitly[Encoder[C]])
      override def <*>[C: Encoder](expr3: Expr[C]): Map3Builder[C] =
        new Map3Builder[C] {
          override def |>[D: Encoder](f: (A, B, C) => D): Expr[D] =
            Map3[A, B, C, D](expr1, expr2, expr3, f, implicitly[Encoder[D]])
        }
    }
  }

}

object Expr {

  type UntypedExpr = Expr[Any]

  implicit def pathToExpr(path: Path): UntypedExpr = {
    import io.univalence.typedpath._

    path match {
      case f: FieldPath => Expr.Ops.RootField(f)
      case ArrayPath(_) => throw new Exception("repetition path (array) not supported in fenek")
    }

  }

  case class Struct(fields: Seq[StructField]) extends UntypedExpr

  case class CaseWhen[B](source: UntypedExpr, cases: CaseWhenExpr[B]) extends Expr[B]

  case class CaseWhenExpr[+B] protected[fenek] (pairs: Seq[(UntypedExpr, Expr[B])], orElse: Option[Expr[B]]) {

    def orWhen[C >: B](caseWhenExpr: CaseWhenExpr[C]): CaseWhenExpr[C] = CaseWhenExpr.merge(this, caseWhenExpr)
  }

  implicit def lit[T: Encoder](t: T): Expr[T] = Lit(t, implicitly[Encoder[T]])

  object CaseWhenExpr {
    def single[A, B](a: Expr[A], b: Expr[B]): CaseWhenExpr[B] = CaseWhenExpr((a, b) :: Nil, None)

    def apply[B](caseWhenExpr1: CaseWhenExpr[B],
                 caseWhenExpr2: CaseWhenExpr[B],
                 more: CaseWhenExpr[B]*): CaseWhenExpr[B] =
      (caseWhenExpr2 +: more).foldLeft(caseWhenExpr1)(_ orWhen _)

    /**
      * Convert implicitly values like 1 -> 1 or path"a" -> path"b" or ... to CaseWhenExpr
      * @param t (left, right)
      * @param left how to convert left to an Expr
      * @param right how to convert right to an Expr
      * @tparam A type of left
      * @tparam B type of right
      * @tparam C type of return
      * @return
      */
    implicit def t2ToCaseWhenExpr[A, B, C](t: (A, B))(implicit left: ToExpr[A],
                                                      right: ToExpr.Aux[B, C]): CaseWhenExpr[C] =
      CaseWhenExpr.single(left.expr(t._1), right.expr(t._2))

    /**
      * Convert implicitly values like Else -> 1 or Else -> path"a" or ... to CaseWhenExpr
      * @param t (Else, right)
      * @param toExpr how to convert right to an Expr
      * @tparam B type of right
      * @tparam C type of return
      * @return
      */
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

  }

  case class StructField(name: String, source: UntypedExpr)

  object StructField

  sealed trait Ops extends UntypedExpr

  object Ops {
    case class Remove[B](source: Expr[B], toRemove: Seq[Expr[B]]) extends Expr[B]
    case class LastElement(expr: UntypedExpr) extends UntypedExpr
    case class RootField(path: FieldPath) extends UntypedExpr
    case class SelectField(path: FieldPath, source: UntypedExpr) extends Ops
    case class Size(source: UntypedExpr) extends Ops
    case class DateDiff(datepart: Expr[String], startdate: UntypedExpr, enddate: UntypedExpr) extends Ops
    case class Left(characterExpr: Expr[String], n: Expr[Int]) extends Expr[String]
    case class DateAdd(interval: Expr[String], n: Expr[Int], date: UntypedExpr) extends Expr[String]
    case class OrElse[B](expr: Expr[B], expr2: Expr[B]) extends Expr[B]
    case class FirstElement(expr: UntypedExpr) extends Ops
    case class TypeCasted[A](source: UntypedExpr, enc: Encoder[A]) extends Expr[A]

    case class JsonMap[O](source: UntypedExpr, f: JValue => O, enc: Encoder[O]) extends Expr[O]

    case class Map1[S, O](source: Expr[S], f: S => O, enc: Encoder[O]) extends Expr[O] {
      def tryApply(a: Any): Try[O] = Try(f(a.asInstanceOf[S]))
    }

    case class Map2[A, B, C](expr1: Expr[A], expr2: Expr[B], f: (A, B) => C, enc: Encoder[C]) extends Expr[C] {
      def tryApply(a: Any, b: Any): Try[C] =
        Try(f(a.asInstanceOf[A], b.asInstanceOf[B]))
    }

    case class Map3[A, B, C, D](
      expr1: Expr[A],
      expr2: Expr[B],
      expr3: Expr[C],
      f: (A, B, C) => D,
      enc: Encoder[D]
    ) extends Expr[D] {
      def tryApply(a: Any, b: Any, c: Any): Try[D] =
        Try(f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C]))
    }

    case class Lit[T](value: T, enc: Encoder[T]) extends Expr[T]
    case class IsEmpty(expr: UntypedExpr) extends Expr[Boolean]

  }

  object interval {
    def day: Expr[String] = Expr.lit("day")
  }

}
object Null extends UntypedExpr

object struct {
  @deprecated("use struct(\"a\" <<- expr) directly", "0.3")
  def build(call: (String, UntypedExpr)*): Expr.Struct = Expr.Struct(call.map((Expr.StructField.apply _).tupled))

  def apply(structField: StructField*): Expr.Struct = Expr.Struct(structField)
}

object Else
