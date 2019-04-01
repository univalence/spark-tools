package io.univalence.fenek.generic

import io.univalence.fenek.Expr.Ops.Lit
import io.univalence.fenek.Expr.{ StructField, UntypedExpr }

sealed trait GenericExpr {
  import GenericExpr.{ Named, OneOrMore }
  def expr: Named[UntypedExpr]
  def sources: Seq[Named[OneOrMore[GenericExpr]]]
  def strs: Seq[Named[String]]
  def values: Seq[Named[Any]]
}

object GenericExpr {

  type OneOrMore[T] = ::[T]

  def OneOrMore[T](t: T, ts: T*): OneOrMore[T] = ::(t, ts.toList)

  case class Named[+T](name: String, value: T)

  def apply(sourceExpr: UntypedExpr): GenericExpr = {
    val named = toNamedSeq(sourceExpr)

    new GenericExpr {
      override lazy val expr: Named[UntypedExpr] =
        Named(sourceExpr.getClass.getName, sourceExpr)

      override lazy val sources: Seq[Named[OneOrMore[GenericExpr]]] = {
        val res = named.collect({
          case Named(name, expr: UntypedExpr) =>
            Named(name, OneOrMore(apply(expr)))
          // a Field could be a expression as well
          case Named(name, StructField(fname, source)) =>
            Named(name + "." + fname, OneOrMore(apply(source)))
          case Named(name, (expr1: UntypedExpr, expr2: UntypedExpr)) =>
            Named(name, OneOrMore(apply(expr1), apply(expr2)))
        })

        res
      }

      override lazy val strs: Seq[Named[String]] = {
        named.collect({ case Named(name, s: String) => Named(name, s) })
      }
      override lazy val values: Seq[Named[Any]] = {
        expr.value match {
          case Lit(value, _) => List(Named("value", value))
          case _             => Nil
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
