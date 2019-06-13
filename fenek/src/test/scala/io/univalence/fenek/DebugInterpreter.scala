package io.univalence.fenek

import io.univalence.fenek.Expr.Ops._
import io.univalence.fenek.Expr._
import org.joda.time.{ Days, Months }
import org.json4s.JsonAST._
import io.univalence.strings._

import scala.collection.BitSet
import scala.util.{ Failure, Success, Try }

object DebugInterpreter {

  def rewrite(expr: UntypedExpr): UntypedExpr = {
    object LocalDate {
      def unapply(arg: String): Option[org.joda.time.LocalDate] = Try(org.joda.time.LocalDate.parse(arg)).toOption
    }
    object ExtractInt {
      def unapply(s: String): Option[Int] = Try(s.toInt).toOption
    }

    expr match {

      case Expr.Ops.Left(source, n) => source.as[String] <*> n |> (_ take _)
      case DateAdd(interval, n, source) =>
        interval <*> n <*> source.as[String] |> {
          case ("day", days, LocalDate(start))     => start.plusDays(days).toString
          case ("month", months, LocalDate(start)) => start.plusMonths(months).toString
          case ("year", years, LocalDate(start))   => start.plusYears(years).toString
        }

      case DateDiff(datepart, startdate, enddate) =>
        datepart <*> startdate.as[String] <*> enddate.as[String] |> {
          case ("day", LocalDate(start), LocalDate(end))   => Days.daysBetween(start, end).getDays
          case ("month", LocalDate(start), LocalDate(end)) => Months.monthsBetween(start, end).getMonths
        }

      case Remove(source, toRemove) =>
        Expr.CaseWhen(source, CaseWhenExpr(toRemove.map(_ -> Null), Some(source)))

      case _ => expr
    }
  }

  object Tools {
    def anyToJValue(any: Any): Try[JValue] =
      Try {
        any match {
          case s: String  => JString(s)
          case b: Boolean => JBool(b)
          case i: Int     => JInt(i)

        }
      }

    def jValueToAny(jvalue: JValue): Try[Any] =
      jvalue match {
        case JString(s) => Try(s)
        case JInt(s)    => Try(s.toInt)
        case JBool(s)   => Try(s)
        case _          => Failure(new Exception("[" + jvalue + "] can't be converted to Scala values"))
      }

  }
  case class TxRes[T <: JValue](result: Try[T], usedExpr: BitSet)

  def tx(struct: Struct): JObject => TxRes[JObject] = {
    //def tx(struct: Fnk.Struct): JObject => (Result[JObject], BitSet)

    case class Compute(nbExpr: Int, tx: JValue => TxRes[JValue])

    def compute(expr: UntypedExpr, index: Int): Compute = {

      println(index.formatted("%03d") + expr)
      val subIndex = index + 1

      rewrite(expr) match {
        case Lit(a, _) => Compute(1, _ => TxRes(Tools.anyToJValue(a), BitSet(index)))

        case RootField(name) =>
          Compute(1, jobj => {
            TxRes(jobj \\ name.toString match {
              case JObject(Nil) => Failure(new Exception(s"missing field $name"))
              case x            => Success(x)
            }, BitSet(index))
          })

        case map @ Map2(first, second, _, _) =>
          val c1 = compute(first, subIndex)
          val c2 = compute(second, c1.nbExpr + subIndex)

          Compute(
            1 + c1.nbExpr + c2.nbExpr,
            jVal => {
              val eval1: TxRes[JValue] = c1.tx(jVal)
              val eval2: TxRes[JValue] = c2.tx(jVal)

              val res = for {
                a0 <- eval1.result
                a1 <- Tools.jValueToAny(a0)

                b0 <- eval2.result
                b1 <- Tools.jValueToAny(b0)

                c0 <- map.tryApply(a1, b1)
                c1 <- Tools.anyToJValue(c0)
              } yield c1

              TxRes(res, BitSet(index) ++ eval1.usedExpr ++ eval2.usedExpr)
            }
          )

        case TypeCasted(source, enc) => {
          val computeSource = compute(source, subIndex)
          Compute(1 + computeSource.nbExpr, jval => {
            val res = computeSource.tx(jval)
            res.copy(usedExpr = res.usedExpr + index)
          })
        }

        case CaseWhen(source, ifes) =>
          val computeSource = compute(source, subIndex)

          val nIndex0: Int = subIndex + computeSource.nbExpr

          val pairs = ifes.pairs
            .foldLeft(nIndex0 -> Seq.empty[(Compute, Compute)])({
              case ((idx, xs), (lhs, rhs)) => {
                val computeLhs = compute(lhs, idx)
                val computeRhs = compute(rhs, idx + computeLhs.nbExpr)

                (idx + computeLhs.nbExpr + computeRhs.nbExpr, xs :+ (computeLhs -> computeRhs))
              }
            })
            ._2

          val sum = pairs.map(t => t._1.nbExpr + t._2.nbExpr).sum

          val orElse: Option[Compute] = ifes.orElse.map(expr => compute(expr, nIndex0 + sum))

          //val nIndex1: Int = orElse.map(_.nbExpr + nIndex0).getOrElse(nIndex0)

          Compute(
            1 + orElse.map(_.nbExpr).getOrElse(0) + sum + computeSource.nbExpr,
            jobj => {

              val source = computeSource.tx(jobj)

              case class State(set: BitSet, selected: Option[Compute])
              val zeroState = State(BitSet(index), None)

              val finalState = pairs.foldLeft(zeroState)({
                case (state, (lhs, rhs)) => {
                  if (state.selected.isDefined)
                    state
                  else {
                    val computedLhs = lhs.tx(jobj)
                    State(state.set ++ computedLhs.usedExpr, if (computedLhs.result == source.result) {
                      Some(rhs)
                    } else None)
                  }
                }
              })

              val res: Option[TxRes[JValue]] = finalState.selected.orElse(orElse).map(compute => compute.tx(jobj))

              TxRes(res.map(_.result).getOrElse(Try(JNothing)),
                    usedExpr = source.usedExpr ++ finalState.set ++ res.map(_.usedExpr).getOrElse(BitSet()))
            }
          )

      }

    }

    val index = 0
    val expr  = struct

    type Out = Seq[(String, Compute)]
    val zeroOut: Out = Nil

    def combineOut(x: Out, name: String, y: Compute): Out = x :+ (name -> y)

    def finalize(out: Out): JObject => TxRes[JObject] = { jobj =>
      {

        val res: Seq[(String, TxRes[JValue])] = out.map({ case (name, compute: Compute) => (name, compute.tx(jobj)) })
        val sets: BitSet                      = res.map(_._2.usedExpr).reduce(_ ++ _)

        val nJobj = JObject((for {
          (name, tx) <- res
          r          <- tx.result.toOption
          r          <- r.toSome
        } yield (name, r)).toList)
        TxRes(Try(nJobj), sets)
      }
    }

    val res = expr.fields.foldLeft[(Int, Out)]((index + 1, zeroOut))({
      case ((idx, out), StructField(name, source)) =>
        val compute0 = compute(source, idx)
        (idx + compute0.nbExpr, combineOut(out, name, compute0))
    })

    finalize(res._2)
  }

  def main(args: Array[String]): Unit = {

    import io.univalence.strings.Key._

    val a: UntypedExpr = key"a"
    val b: UntypedExpr = key"b"

    val ab = a.as[Int] <*> b.as[Int]

    val x = key"a" caseWhen (1 -> (ab |> (_ + _)), 2 -> (ab |> ((a, b) => { println("toto"); a - b })), Else -> 3)

    val function = tx(struct("x" <<- x, "y" <<- x.as[Int]))

    import org.json4s._
    import org.json4s.native.JsonMethods._

    implicit def jobject(str: String): JObject = parse(str, useBigDecimalForDouble = false).asInstanceOf[JObject]

    println(function("""{"a":1,"b":0}"""))

    println(function("""{"a":2,"b":0}"""))

    println(function("""{"a":3,"b":0}"""))

  }
}
