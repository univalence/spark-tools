package fix

import io.univalence.fenek._
import io.univalence.fenek.Expr._
import io.univalence.typedpath.Path._

object Fenek2to3All {

  //Types
  type T1 = Expr[Any]
  type T2 = Expr[String]

  //The struct syntax
  val expr: Expr[Any] = ???
  val somestruct: Struct = struct("abc"  <<- expr)


  //The >.syntax
  val expr2: Expr[String] = path"toto.tata.titi".as[String] |> (_ + "!")


  //The caseWhen syntax
  val expr3: Expr[Int] = expr caseWhen (1 -> 2 , 2 -> 3)


  val dm = path"dm"

  val code = path"$dm.pack.code"

  val combo: Struct = struct("abc"  <<- path"toto")
}
