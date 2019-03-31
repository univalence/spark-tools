package io.univalence.fenek

import org.scalatest.FunSuite

class DslTest extends FunSuite {
  import Fnk._

  test("basic") {

    val x: TypedExpr[Boolean] = (lit(true) caseWhen (true -> true, false -> true)).as[Boolean]

    val y: Expr = lit(2).caseWhen(1 -> "b", Else -> 0)

    val z: Expr = lit(1).caseWhen(1 -> "a", 2 -> "b").as[String] |> (_.toUpperCase)

  }

  test("simple") {
    val expr: Expr = Fnk.lit(0)

    val rule0: Expr.CaseWhenExpr = 1 -> "a" | 2 -> 1 | 3 -> true

    val rule4: Expr.CaseWhenExpr = 1 -> 1 | 2 -> 2 | 3 -> "a"

    val rule5: Expr.CaseWhenExpr = 1 -> 1 | 2 -> 2 | Else -> expr

    val rule2: Expr.CaseWhenExprTyped[Int] = 1 -> 2 | 2 -> 3 | Else -> 4

    val rule1: Expr.CaseWhenExprTyped[Int] = true -> 2 | false -> 3

    //TODO : Fix the types
    val e1: TypedExpr[Int] = (lit(true) caseWhen rule1 caseWhen rule2).as[Int]

    val e2: TypedExpr[Int] = (lit(true) caseWhen 1 -> 2).as[Int]

    val e4: Expr = lit(true) caseWhen rule0
  }
}
