package io.univalence

import io.univalence.fenek.Expr.{ StructField, UntypedExpr }
import io.univalence.typedpath.Key

package object fenek {
  implicit class fieldOps(name: String) {
    def <<-(expr: UntypedExpr): StructField = StructField(name, expr)
    //def <<-(path: Path): StructField        = StructField(name, path)
  }

  implicit class booleanOps(_expr: Expr[Boolean]) {
    def or(expr: Expr[Boolean]): Expr[Boolean]  = _expr <*> expr |> (_ || _)
    def and(expr: Expr[Boolean]): Expr[Boolean] = _expr <*> expr |> (_ && _)
  }
}
