package io.univalence.fenek

import io.univalence.fenek.Fnk.Expr.Ops.CaseWhen
import org.json4s.JsonAST._
import org.scalatest.FunSuite

import scala.language.dynamics
import scala.language.implicitConversions

class JsonInterpreterTest extends FunSuite {
  type Struct = Fnk.Expr.Struct

  import Fnk._

  val in: Fnk.>.type = >

  sealed trait StructChecker {

    private def self: StructChecker.StructCheckerImpl =
      this.asInstanceOf[StructChecker.StructCheckerImpl]

    object setInput extends Dynamic {
      def applyDynamicNamed(method: String)(call: (String, Any)*): StructChecker =
        self.copy(inputs = call.foldLeft(self.inputs)(_ + _))
    }

    object setExpected extends Dynamic {
      def applyDynamicNamed(method: String)(calls: (String, Any)*): StructChecker = {

        val keys = calls.map(_._1).toSet

        self.copy(outputs = calls ++ self.outputs.filterNot(x => keys(x._1)))
      }

    }

    def check(): Unit = {

      def anyToJValue(any: Any): JValue =
        any match {
          case s: String      => JString(s)
          case a: JValue      => a
          case i: Int         => JInt(i)
          case b: Boolean     => JBool(b)
          case d: Double      => JDouble(d)
          case de: BigDecimal => JDecimal(de)
        }

      val ts = self.struct
      val in = JObject(self.inputs.toList.map({
        case (k, v) => (k, anyToJValue(v))
      }))

      val res = ts.tx(in)

      val obj: JObject = res.value.get

      //res.annotations.foreach(println)

      self.outputs.foreach({
        case (n, v) =>
          val found = obj.obj.find(_._1 == n)

          assert(found.nonEmpty, n)
          assert(found.get._2 == anyToJValue(v), n)
      })
    }

  }

  object StructChecker {

    case class StructCheckerImpl(struct: Struct, inputs: Map[String, Any], outputs: Seq[(String, Any)])
        extends StructChecker

  }

  implicit class StructOps(ts: Struct) {

    object setInput extends Dynamic {

      def applyDynamicNamed(method: String)(call: (String, Any)*): StructChecker =
        StructChecker.StructCheckerImpl(ts, call.foldLeft(Map.empty[String, Any])((m, kv) => m + kv), Seq.empty)
    }

    object setExpected extends Dynamic {
      def applyDynamicNamed(method: String)(call: (String, Any)*): StructChecker =
        StructChecker.StructCheckerImpl(ts, Map.empty, call)
    }

    def check(in: JObject, out: JObject): Unit = {
      val res = ts.tx(in)

      val obj: JObject = res.value.get

      //res.annotations.foreach(println)

      out.obj.foreach(x => {
        val found = obj.obj.find(_._1 == x._1)

        assert(found.nonEmpty, x)
        assert(found.get._2 == x._2, x._1)

      })

    }

    def tx(jobj: JObject): JsonInterpreter.Result[JObject] =
      JsonInterpreter.tx(ts)(jobj)
  }

  implicit def jsonConversionToStringInTheContextOfThisFile(str: String): JObject = {
    import org.json4s.native.JsonMethods._
    parse(str, useBigDecimalForDouble = true).asInstanceOf[JObject]
  }

  test("casewhen bug #2") {

    val daValidVente = lit("2019-01-01")

    val dateparution = """2019-01-02"""

    val est_annulé = lit(false)

    val expr1 = >.gppTypeProduit.caseWhen("KTTR" -> >.ktStartCommitmentDate | Else -> dateparution)
    val expr2 = >.ktInvoicingType.caseWhen(
      "STANDARD" -> est_annulé.caseWhen(true -> dateparution | false -> >.ktStartCommitmentDate) |
        Else     -> expr1
    )
    val da_deb_periode = >.gppTypeProduit
      .caseWhen("KTREMB" -> daValidVente | "KTREGU" -> daValidVente) orElse
      expr2

    val tx =
      struct(da_deb_periode = da_deb_periode, expr1 = expr1, expr2 = expr2)

    tx.setInput(ktInvoicingType = "MONTHLY", gppTypeProduit = "TOTO")
      .setExpected(
        expr1          = dateparution,
        expr2          = dateparution,
        da_deb_periode = dateparution
      )
      .check()

  }

  test("case when bug #2 reduction") {
    val expr = lit(1).caseWhen(Else -> lit(1).caseWhen(2 -> Null | Else -> 1))

    assert(expr.asInstanceOf[CaseWhen].ifes.orElse.nonEmpty)

    val tx = struct(expr = expr)

    tx.setInput.check(expr = 1)

    //Test DSL

    val s: StructChecker = tx.setInput(a = 2, b = 1)

    s.setExpected(expr = 1).check()
    s.setExpected(expr = 0).setExpected(expr = 1).check()

  }

  test("<*> & |> avec int operation not working") {
    val factIterationNumber: TypedExpr[Int] = 13
    val data12: TypedExpr[Int]              = 12

    val isTR: TypedExpr[Boolean] =
      (lit(13) <*> lit(12) |> (_ % _) caseWhen (Else -> false | 1 -> true))
        .as[Boolean]

    struct(expr = isTR).setExpected(expr = true).check()

  }

  test("lit(true).as[Boolean]") {

    struct(a = lit(true).as[Boolean]).setExpected(a = true).check()

  }

  test("cast str to in") {
    struct(a = lit("1").as[Int]).setExpected(a = 1).check()
  }

  test("<*> & |> avec int operation not working 2") {
    val factIterationNumber: TypedExpr[Int] = >.iterationinvoicenumber.as[Int]
    val data12: TypedExpr[Int]              = 12
    val TR =
      (factIterationNumber <*> data12 |> (_ % _) caseWhen (Else -> false | 1 -> true))
        .as[Boolean]

    val tx = struct(expr = TR)
    val s: StructChecker = tx
      .setInput(iterationinvoicenumber = 13)
      .setExpected(expr = true)

    s.check()
  }

  test("""lit("1").as[Int] <*> lit("0").as[Int] |> ( _+_ )""") {

    struct(a = lit("1").as[Int] <*> lit("2").as[Int] |> (_ + _))
      .setExpected(a = 3)
      .check()

  }

  test("testTx") {

    val tx = struct.create(b = in.a, c = lit(2), d = in.a)

    tx.check(in = """{"a":1}""", out = """{"b":1, "c":2, "d" :1}""")
    //tx.check2("a" -> 1)("b" -> 1,"c" -> 2,"d" -> 1)

    //tx.check2(a = 1)(b = 1,c = 2,d = 1)

  }

  test("cleanDate") {

    val tx = struct.create(dt = in.dt.left(10))

    tx.check(
      """
   {"dt" : "2018-10-25T15:00:31+00:00"}
""",
      """
{"dt" : "2018-10-25"}
"""
    )

    tx.check("""{"a":0}""", """{}""")
  }

  test("addDate") {

    val tx = struct.create(dt = in.dt.left(10).dateAdd("day", 1))

    tx.check(
      """
   {"dt" : "2018-10-25T15:00:31+00:00"}
""",
      """
{"dt" : "2018-10-26"}
"""
    )

    tx.check("""{"a":0}""", """{}""")
  }

  test("datediff") {

    val tx = struct(interval = in.start.datediff("month", in.end))

    tx.check(
      """
   {"end" : "2018-10-25", "start" : "2018-10-24"}
""",
      """
   {"interval" : 0}

"""
    )

    tx.check(
      """
   {"end" : "2019-01-11", "start" : "2017-02-12"}
""",
      """
   {"interval" : 22}

"""
    )
  }

  test("niveauPack") {

    val tx = struct(niveauPack = in.configuration #> {
      case JArray(arr) => {
        arr
          .map(x => x \\ "niveauPack")
          .collect({ case JString(s) => s })
          .mkString(", ")
      }
    })

    tx.check(
      """{ "configuration" : [{"niveauPack": "PACK_2"},{"niveauPack":"PACK_3"}]}""",
      """{"niveauPack": "PACK_2, PACK_3"}"""
    )

  }

  test("when") {

    val tx = struct.create(toto = in.tata.caseWhen(false -> "titi"))

    tx.check("""{"tata":false}""", """{"toto":"titi"}""")

  }

  test("nested") {
    struct.create(c = in.a.>.b).check("{\"a\":{\"b\":1}}", "{\"c\":1}")
  }

  test("isEmpty") {
    struct.create(c = in.a.isEmpty).check("""{"a":"1"}""", """{"c":false}""")
    struct.create(c = in.a.isEmpty).check("""{"b":"1"}""", """{"c":true}""")

  }

  //TODO
  test("caseWhen") {
    struct
      .create(c = in.a.caseWhen(true -> "ok" | false -> "no"))
      .check("""{"a":true}""", """{"c":"ok"}""")
    struct
      .create(c = in.a.caseWhen(true -> "ok" | false -> "no"))
      .check("""{"a":false}""", """{"c":"no"}""")
    struct
      .create(c = in.a.caseWhen(true -> "ok" | false -> "no"))
      .check("""{"a":1}""", """{}""")
  }

}
