package io.univalence.fenek.beta

import io.univalence.fenek.Fnk
import io.univalence.fenek.Fnk.Expr
import io.univalence.fenek.Fnk.Expr.Ops
import io.univalence.fenek.Fnk.Expr.Ops.RootField
import io.univalence.fenek.Fnk.TypedExpr.Lit
import io.univalence.fenek.Fnk.TypedExpr.Map2
import io.univalence.fenek.Fnk.TypedExpr.TypeCasted

object StaticAnalysis {

  sealed trait Movement

  object Movement {

    case object Up extends Movement

    case object Down extends Movement

    case object Same extends Movement

  }

  type Tree[T] = Seq[(Movement, T)]

  import Movement._

  Seq(Same -> 1, Down -> 2, Down -> 3, Same -> 4, Up -> 5)

  /* 1        1    0 9
     - 2      2    1 6
       - 3    3    2 3
       - 4    4    4 5
     - 5      5    7 8
   */

  /*
      lit(a) caseWhen (1 -> a + b | 2 -> a - b | Else -> 3)

      caseWhen
      - a
      - caseWhenExp
        - valueEqual
          - 1
          - +
            - a
            - b
        - valueEqual
          - 2
          - -
            - a
            - b
        - elseCase
          - 3
   */

  case class PosExpr(level: Int, index: Int, expr: Expr)

  def staticAnalysis(expr: Expr): Seq[PosExpr] = {
    def loop(expr: Expr, pos: Int, index: Int): Seq[PosExpr] = {

      val toUnfold: Seq[Expr] = expr match {
        case cw: Ops.CaseWhen =>
          Seq(Seq(cw.source), cw.ifes.pairs.flatMap(t => Seq(t._1, t._2)), cw.ifes.orElse.toList).flatten

        case x: RootField => Nil
        case l: Lit[_]    => Nil

        case Map2(a, b, _, _) => Seq(a, b)

        case TypeCasted(a, _) => Seq(a)
      }

      def next(expr: Expr, index: Int): Seq[PosExpr] =
        loop(expr, pos + 1, index)

      val res = toUnfold.foldLeft[(Seq[PosExpr], Int)]((Nil, index + 1))(
        {
          case ((xs, i), e) => {
            val ys = next(e, i)
            (xs ++ ys, i + ys.size)
          }
        }
      )

      PosExpr(pos, index, expr) +: res._1

    }

    loop(expr, pos = 0, 0)
  }

  def main(args: Array[String]): Unit = {

    import Fnk._

    val ab: TypedExpr[Int]#Map2Builder[Int] = >.a.as[Int] <*> >.b.as[Int]

    val x = >.a caseWhen (1 -> (ab |> (_ + _)) | 2 -> (ab |> (_ - _)) | Else -> 3)

    staticAnalysis(x).foreach({
      case PosExpr(level, index, expr) =>
        println(index.formatted("%03d ") + ("  " * level) + expr)
    })

  }

}
