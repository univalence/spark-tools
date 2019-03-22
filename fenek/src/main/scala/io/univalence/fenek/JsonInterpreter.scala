package io.univalence.fenek

import Fnk.Expr.CaseWhenExprUnTyped
import Fnk.Encoder
import Fnk.Expr
import Fnk.TypedExpr
import org.joda.time.Days
import org.joda.time.Months
import org.json4s.JsonAST._

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object JsonInterpreter {
  def directTx(struct: Fnk.Expr.Struct): JObject => Try[JObject] = {
    val f: JObject => Result[JObject] = tx(struct)

    f.andThen(x => Try(x.value.get))
  }

  sealed trait Annotation {
    def scope: Seq[String]
    def addScope(name: String): Annotation
  }

  case class MissingField(name: String, scope: Seq[String] = Nil) extends Annotation {
    override def addScope(name: String): MissingField =
      copy(scope = name +: scope)
  }

  case class CaughtException(exception: Throwable, scope: Seq[String] = Nil) extends Annotation {
    override def addScope(name: String): CaughtException =
      copy(scope = name +: scope)
  }

  case class Result[+T](value: Option[T], annotations: Seq[Annotation]) {
    def addScope(name: String): Result[T] =
      copy(annotations = annotations.map(_.addScope(name)))
    def map[B](f: T => B): Result[B] = Result(value.map(f), annotations)

    def flatMap[B](f: T => Result[B]): Result[B] = {
      val res: Option[Result[B]] = value.map(f)
      Result(res.flatMap(_.value), res.map(_.annotations).getOrElse(Nil) ++ annotations)
    }

    def addAnnotation(annotation: Annotation*): Result[T] =
      Result(value, annotation ++ annotations)
  }

  object Result {

    val empty: Result[Nothing] = Result(None, Nil)

    def point[T](t: T): Result[T] = t match {
      case JNothing => empty
      case _        => Result(Option(t), Nil)
    }
  }

  def tx(struct: Fnk.Struct): JObject => Result[JObject] = {
    //def tx(struct: Fnk.Struct): JObject => (Result[JObject], BitSet)

    import Fnk.Expr.Ops._
    import Fnk.TypedExpr._

    def anyToJValue(any: Any): Try[JValue] =
      Try {
        any match {
          case s: String      => JString(s)
          case b: Boolean     => JBool(b)
          case i: Int         => JInt(i)
          case d: Double      => JDouble(d)
          case de: BigDecimal => JDecimal(de)

        }
      }

    def jValueToAny(jvalue: JValue): Try[Any] =
      jvalue match {
        case JString(s)  => Try(s)
        case JInt(s)     => Try(s.toInt)
        case JBool(s)    => Try(s)
        case JDouble(s)  => Try(s)
        case JDecimal(s) => Try(s)
        case _ =>
          Failure(new Exception("[" + jvalue + "] can't be converted to Scala values"))
      }

    object LocalDate {
      def unapply(arg: String): Option[org.joda.time.LocalDate] =
        Try(org.joda.time.LocalDate.parse(arg.take(10))).toOption
    }

    def rewrite(expr: Expr): Expr =
      expr match {

        case TypedExpr.CaseWhen(source, cases) =>
          Fnk.Expr.Ops.CaseWhen(source, cases)

        case Left(source, n) => source.as[String] <*> n |> (_ take _)
        case DateAdd(interval, n, source) =>
          interval <*> n <*> source.as[String] |> {
            case ("day", days, LocalDate(start)) =>
              start.plusDays(days).toString
            case ("month", months, LocalDate(start)) =>
              start.plusMonths(months).toString
            case ("year", years, LocalDate(start)) =>
              start.plusYears(years).toString
          }

        case DateDiff(datepart, startdate, enddate) =>
          datepart <*> startdate.as[String] <*> enddate.as[String] |> {
            case ("day", LocalDate(start), LocalDate(end)) =>
              Days.daysBetween(start, end).getDays
            case ("month", LocalDate(start), LocalDate(end)) =>
              Months.monthsBetween(start, end).getMonths
          }

        case Remove(source, toRemove) =>
          Expr.Ops.CaseWhen(source, CaseWhenExprUnTyped(toRemove.map(_ -> Fnk.Null), Some(source)))

        case _ => expr
      }

    implicit def tryToResult[T](t: Try[T]): Result[T] = t match {
      case Success(v) => Result(Some(v), Nil)
      case Failure(e) => Result(None, CaughtException(e, Nil) :: Nil)
    }

    def rawJson(source: Expr, f: JValue => JValue): JValue => Result[JValue] = {
      val f1 = compute(source)
      f1.andThen(
          x =>
            x.value match {
              case None => x.copy(value = Some(JNothing))
              case _    => x
          }
        )
        .andThen(_.flatMap(x => Try(f(x))))
    }

    object ExtractInt {
      def unapply(s: String): Option[Int] = Try(s.toInt).toOption
    }

    def compute(expr: Expr): JValue => Result[JValue] = {
      rewrite(expr) match {
        case IsEmpty(source) =>
          val f = compute(source)
          rawJson(source, {
            case JString("") | JNothing => JBool(true); case _ => JBool(false)
          })

        // LastElement can't be rewriten, it would need a way to specify the schema of the ouput, even using #>
        case LastElement(source) =>
          rawJson(source, { case JArray(arr) => arr.last })

        case FirstElement(source) =>
          rawJson(source, { case JArray(arr) => arr.head })

        case Size(source) =>
          rawJson(source, {
            case JString(s)  => JInt(s.length)
            case JArray(arr) => JInt(arr.size)
          })

        case JsonMap(source, jSonToValue, _) =>
          rawJson(source, jSonToValue andThen anyToJValue andThen (_.get))

        case map @ Map3(first, second, third, _, _) =>
          val f1 = compute(first)
          val f2 = compute(second)
          val f3 = compute(third)

          jVal =>
            {
              for {
                a <- f1(jVal).flatMap(jValueToAny)

                b <- f2(jVal).flatMap(jValueToAny)

                c <- f3(jVal).flatMap(jValueToAny)

                r <- map.tryApply(a, b, c)
                r <- anyToJValue(r)
              } yield r

            }

        case map @ Map2(first, second, _, _) =>
          val f1 = compute(first)
          val f2 = compute(second)

          jVal =>
            {
              for {
                a <- f1(jVal).flatMap(jValueToAny)

                b <- f2(jVal).flatMap(jValueToAny)

                r <- map.tryApply(a, b)
                r <- anyToJValue(r)
              } yield r
            }

        case map @ Map1(first, _, _) =>
          val f1 = compute(first)

          jVal =>
            {
              for {
                a <- f1(jVal).flatMap(jValueToAny)

                r <- map.tryApply(a)
                r <- anyToJValue(r)
              } yield r
            }

        //TODO improve error reporting on typecasting
        case TypeCasted(source, enc) =>
          compute(source).andThen(_.map({
            //SAME
            case v: JString if enc == Encoder.Str         => v
            case v: JBool if enc == Encoder.Bool          => v
            case v: JInt if enc == Encoder.Int            => v
            case v: JDouble if enc == Encoder.Double      => v
            case v: JDecimal if enc == Encoder.BigDecimal => v
            case JInt(x) if enc == Encoder.BigDecimal     => JDecimal(BigDecimal(x))
            case JInt(x) if enc == Encoder.Double         => JDouble(x.toDouble)
            //Int => String
            case JString(ExtractInt(i)) if enc == Encoder.Int => JInt(i)
            case JString(s) if enc == Encoder.BigDecimal =>
              JDecimal(BigDecimal(s))
            //Boolean => String
            case JString("true") if enc == Encoder.Bool  => JBool(true)
            case JString("false") if enc == Encoder.Bool => JBool(false)
            //X => String
            case JBool(x) if enc == Encoder.Str => JString(x.toString)
            case JInt(x) if enc == Encoder.Str  => JString(x.toString)
            //Casting didn't work
            case _ => JNothing
          }))

        case Lit(t, _) =>
          _ =>
            anyToJValue(t)

        case OrElse(exp1, exp2) =>
          val f1 = compute(exp1)
          val f2 = compute(exp2)
          jVal =>
            val res = f1(jVal)
            res.value match {
              case None | Some(JNothing) =>
                f2(jVal).addAnnotation(res.annotations: _*)
              case _ => res
            }

        case Field(name) =>
          jobj =>
            jobj \\ name match {
              case JObject(Nil) => Result(None, MissingField(name) :: Nil)
              case x            => Result(Some(x), Nil)
            }

        case SelectField(name, source) =>
          val f1 = compute(source)
          jvalue =>
            {
              f1(jvalue).flatMap(
                y =>
                  y \ name match {
                    case JObject(Nil) => Result(None, MissingField(name) :: Nil)
                    case x            => Result.point(x)
                }
              )
            }

        case Expr.Ops.CaseWhen(source, cases) =>
          val f1: JValue => Result[JValue] = compute(source)

          val elseCase: Option[JValue => Result[JValue]] =
            cases.orElse.map(compute)

          val otherCases: Seq[(JValue => Result[JValue], JValue => Result[JValue])] =
            cases.pairs
              .map({
                case (exp1, exp2) =>
                  compute(exp1) -> compute(exp2)
              })

          jvalue =>
            {
              val input: Result[JValue] = f1(jvalue)

              otherCases
                .collectFirst({
                  case (f2, f3) if f2(jvalue).value == input.value =>
                    f3(jvalue).addAnnotation(input.annotations: _*)
                })
                .getOrElse(
                  elseCase.fold[Result[JValue]](Result.empty)(_(jvalue))
                )
            }

        case Fnk.Null =>
          _ =>
            Result.point(JNull)

      }
    }

    val maps: scala.List[(String, JValue => Result[JValue])] =
      struct.fields
        .map(x => x.name -> compute(x.source).andThen(_.flatMap(Result.point)))
        .toList

    jobj =>
      {
        val res: List[(String, Result[JValue])] = maps.map({
          case (name, f) => (name, f(jobj).addScope(name))
        })

        val allAnnotations: Seq[Annotation] = res.flatMap(_._2.annotations)

        val values: List[(String, JValue)] =
          res.flatMap({ case (name, r) => r.value.map(name -> _) }).toList

        Result(Some(JObject(values)), allAnnotations)

      }

  }

}
