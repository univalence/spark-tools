package io.univalence

import io.univalence.fenek.Expr.StructField

package object fenek {
  implicit class fieldOps(name: String) {
    def <<-(expr: Expr): StructField = StructField(name, expr)
  }
}
