package io.univalence.fenek

import io.univalence.fenek.Expr._
import org.scalatest.FunSuite

class DslTest extends FunSuite {

  test("basic") {

    val x: Expr[Boolean] = true caseWhen (true -> true, false -> true)

    val y: UntypedExpr = 2.caseWhen(1 -> "b", Else -> 0)

    val z: UntypedExpr = 1.caseWhen(1 -> "a", 2 -> "b").as[String] |> (_.toUpperCase)

  }

  test("simple") {
    val expr: UntypedExpr = Expr.lit(0)

    val rule0: Expr.CaseWhenExpr[Any] = CaseWhenExpr(1 -> "a", 2 -> 1, 3 -> true)

    val rule4: Expr.CaseWhenExpr[Any] = CaseWhenExpr(1 -> 1, 2 -> 2, 3 -> "a")

    val rule5: Expr.CaseWhenExpr[Any] = CaseWhenExpr(1 -> 1, 2 -> 2, Else -> expr)

    val rule2: Expr.CaseWhenExpr[Int] = CaseWhenExpr(1 -> 2, 2 -> 3, Else -> 4)

    val rule1: Expr.CaseWhenExpr[Int] = CaseWhenExpr(true -> 2, false -> 3)

    val e1: Expr[Int] = lit(true) caseWhen rule1 caseWhen rule2

    val e2: Expr[Int] = lit(true) caseWhen 1 -> 2

    val e4: UntypedExpr = lit(true) caseWhen rule0
  }
}
