package io.univalence

import io.univalence.fenek.Expr.{ StructField, UntypedExpr }
import io.univalence.typedpath.Path

package object fenek {
  implicit class fieldOps(name: String) {
    def <<-(expr: UntypedExpr): StructField = StructField(name, expr)
    def <<-(path: Path): StructField        = StructField(name, path)
  }
}
