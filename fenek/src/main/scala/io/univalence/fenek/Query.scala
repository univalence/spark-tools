package io.univalence.fenek

import io.univalence.fenek.Expr.Struct

sealed trait Query {
  final def andWhere(expr: Expr[Boolean]): Query = Where(this, expr)
  //final def orWhere(expr: Expr[Boolean]):Query = OrWhere(this,expr)
  final def union(query: Query): Query = Union(this, query)
}

case class Select(projection: Struct) extends Query

case class Union(left: Query, right: Query) extends Query

case class Where(query: Query, where: Expr[Boolean]) extends Query

object Query {
  implicit def structToQuery(struct: Struct): Query = Select(struct)
}
