/*
rules = [
  "Fenek2to3Import"
  "Fenek2to3Path"
  "Fenek2to3Rest"
]
*/
package fix

import io.univalence.fenek._
import io.univalence.fenek.Fnk._
import TypedExpr._

object Fenek2to3All {

  //Types
  type T1 = Expr
  type T2 = TypedExpr[String]

  //The struct syntax
  val expr: Expr = ???
  val somestruct: Struct = struct(abc = expr)


  //The >.syntax
  val expr2: TypedExpr[String] = >.toto.>.tata.>.titi.as[String] |> (_ + "!")


  //The caseWhen syntax
  val expr3: TypedExpr[Int] = expr caseWhen (1 -> 2 | 2 -> 3)


  val dm = >.dm

  val code = dm.>.pack.>.code

  val combo: Struct = struct(abc = >.toto)

  type T3 = Fnk.Expr
}
