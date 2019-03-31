package io.univalence

import io.univalence.fenek.Expr.{ StructField, UntypedExpr }

package object fenek {
  implicit class fieldOps(name: String) {
    def <<-(expr: UntypedExpr): StructField = StructField(name, expr)
  }
}
