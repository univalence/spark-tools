package io.univalence.fenek

import com.fasterxml.jackson.core.JsonParser
import io.univalence.fenek.Expr._
import io.univalence.typedpath._
import org.json4s.JsonAST._
import org.scalatest.FunSuite

import scala.language.implicitConversions



object JsonInterpreterTest {
  type Struct = Expr.Struct

  sealed trait StructChecker {

    private def self: StructChecker.StructCheckerImpl =
      this.asInstanceOf[StructChecker.StructCheckerImpl]

    object setInput {
      def apply(call: (String, Any)*): StructChecker =
        self.copy(inputs = call.foldLeft(self.inputs)(_ + _))
    }

    object setExpected {
      def apply(calls: (String, Any)*): StructChecker = {

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

    object setInput {
      def apply(calls: (String, Any)*): StructChecker =
        StructChecker.StructCheckerImpl(ts, calls.foldLeft(Map.empty[String, Any])((m, kv) => m + kv), Seq.empty)
    }

    object setExpected {
      def apply(calls: (String, Any)*): StructChecker =
        StructChecker.StructCheckerImpl(ts, Map.empty, calls)
    }

    /**
      *
      * @param in l'entrée sous forme de JObject
      * @param outs la ou les sorties attendues de la transformation.
      *             * 0 sortie lors de l'utilisation d'un filtre
      *             * 1 en général
      *             * N lors de l'utilisation d'une union
      */
    def check(in: JObject, outs:JObject*): Unit = {
      val res = ts.tx(in)

      val obj: JObject = res.value.get

      //res.annotations.foreach(println)

      outs.head.obj.foreach(x => {
        val found = obj.obj.find(_._1 == x._1)

        assert(found.nonEmpty, x)
        assert(found.get._2 == x._2, x._1)

      })

    }

    def tx(jobj: JObject): JsonInterpreter.Result[JObject] =
      JsonInterpreter.tx(ts)(jobj)
  }

  implicit def jsonConversionToStringInTheContextOfThisFile(str: String): JObject = {
    import org.json4s.jackson.JsonMethods._

    mapper.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
    parse(str, useBigDecimalForDouble = true).asInstanceOf[JObject]
  }


}


class UnionTest extends FunSuite {

  import JsonInterpreterTest._

  ignore("union") {
    val x = struct("c" <<- path"a")
    val y = struct("c" <<- path"b")

    val z:Struct = Expr.union(x,y)

    z.check("""{a:1, b:2}""", """{c:1}""", """"{c:2}""")

  }

}


class JsonInterpreterTest extends FunSuite {

  import JsonInterpreterTest._
  test("casewhen bug #2") {

    val daValidVente = lit("2019-01-01")

    val dateparution = """2019-01-02"""

    val est_annulé = lit(false)

    import Expr._

    import CaseWhenExpr._
    import CaseWhenExpr.ToExpr._

    val x: CaseWhenExpr[Int] = "a" -> 2
    val y                    = x orWhen x

    val caseWhenExpr2: CaseWhenExpr[Any] = Else -> 1

    val expr1 = path"gppTypeProduit".caseWhen("KTTR" -> path"ktStartCommitmentDate", Else -> dateparution, x)

    val expr2 = path"ktInvoicingType".caseWhen(
      "STANDARD" -> est_annulé.caseWhen(true -> dateparution, false -> path"ktStartCommitmentDate"),
      Else       -> expr1
    )

    val da_deb_periode = path"gppTypeProduit" caseWhen ("KTREMB" -> daValidVente, "KTREGU" -> daValidVente) orElse expr2

    val tx = struct("da_deb_periode" <<- da_deb_periode, "expr1" <<- expr1, "expr2" <<- expr2)

    tx.setInput("ktInvoicingType" -> "MONTHLY", "gppTypeProduit" -> "TOTO")
      .setExpected(
        "expr1"           -> dateparution,
        "expr2"           -> dateparution,
        "da_deb_periode" -> dateparution
      )
      .check()

  }

  test("case when bug #2 reduction") {
    val expr = 1.caseWhen(Else -> 1.caseWhen(2 -> Null, Else -> 1))

    assert(expr.cases.orElse.nonEmpty)

    val tx = struct("expr" <<- expr)

    tx.setInput().setExpected("expr" -> 1)

    //Test DSL

    val s: StructChecker = tx.setInput("a" -> 2, "b" -> 1)

    s.setExpected("expr" -> 1).check()
    s.setExpected("expr" -> 0).setExpected("expr" -> 1).check()

  }

  test("<*> & |> avec int operation not working") {
    val isTR: Expr[Boolean] = 13 <*> 12 |> (_ % _) caseWhen (Else -> false, 1 -> true)

    struct("expr" <<- isTR).setExpected("expr" -> true).check()

  }

  test("lit(true).as[Boolean]") {

    struct("a" <<- lit(true).as[Boolean]).setExpected("a" -> true).check()

  }

  test("cast str to in") {
    struct("a" <<- "1".as[Int]).setExpected("a" -> 1).check()
  }

  test("<*> & |> avec int operation not working 2") {
    val TR = path"iterationinvoicenumber".as[Int] <*> 12 |> (_ % _) caseWhen (Else -> false, 1 -> true)

    val tx = struct("expr" <<- TR)
    val s: StructChecker = tx
      .setInput("iterationinvoicenumber" -> 13)
      .setExpected("expr" -> true)

    s.check()
  }

  test("""lit("1").as[Int] <*> lit("0").as[Int] |> ( _+_ )""") {

    struct("a" <<- (lit("1").as[Int] <*> lit("2").as[Int] |> (_ + _)))
      .setExpected("a" -> 3)
      .check()

  }

  test("testTx") {

    val a  = path"a"
    val tx = struct(path"a" as "b", "b" <<- a, "c" <<- 2, "d" <<- a)

    tx.check(in = """{a:1}""", """{b:1, c:2, d:1}""")
    //tx.check2("a" -> 1)("b" -> 1,"c" -> 2,"d" -> 1)

    //tx.check2(a = 1)(b = 1,c = 2,d = 1)

  }

  test("cleanDate") {

    val tx = struct("dt" <<- path"dt".left(10))

    tx.check(
      """{dt : "2018-10-25T15:00:31+00:00"}""",
      """{dt : "2018-10-25"}"""
    )

    tx.check("""{a:0}""", """{}""")
  }

  test("addDate") {

    val tx = struct("dt" <<- path"dt".left(10).dateAdd("day", 1))

    tx.check(
      """{dt : "2018-10-25T15:00:31+00:00"}""",
      """{dt : "2018-10-26"}"""
    )

    tx.check("""{a:0}""", """{}""")
  }

  test("datediff") {

    val tx = struct("interval" <<- path"start".datediff("month", path"end"))

    tx.check(
      """{end : "2018-10-25", start : "2018-10-24"}""",
      """{interval : 0}"""
    )

    tx.check(
      """{end : "2019-01-11", start : "2017-02-12"}""",
      """{interval : 22}"""
    )
  }

  test("niveauPack") {

    val tx = struct("niveauPack" <<- (path"configuration" #> {
      case JArray(arr) =>
        arr
          .map(x => x \\ "niveauPack")
          .collect({ case JString(s) => s })
          .mkString(", ")
    }))

    tx.check(
      """{configuration: [{niveauPack: "PACK_2"},{niveauPack:"PACK_3"}]}""",
      """{niveauPack: "PACK_2, PACK_3"}"""
    )

  }

  test("when") {
    val tx = struct("toto" <<- path"tata".caseWhen(false -> "titi"))

    tx.check("""{tata:false}""", """{toto:"titi"}""")
  }

  test("nested") {
    struct("c" <<- path"a.b").check("{a:{b:1}}", "{c:1}")
  }

  //p => path
  //j => json  {a:1, b:$b}

  test("isEmpty") {
    struct("c" <<- path"a".isEmpty).check("""{a:1}""", """{c:false}""")

    struct("c" <<- path"a".isEmpty).check("""{b:1}""", """{c:true}""")
  }

  test("caseWhen") {
    struct("c" <<- path"a".caseWhen(true -> "ok", false -> "no"))
      .check("""{a:true}""", """{c:"ok"}""")

    struct("c" <<- path"a".caseWhen(true -> "ok", false -> "no"))
      .check("""{a:false}""", """{"c":"no"}""")

    struct("c" <<- path"a".caseWhen(true -> "ok", false -> "no"))
      .check("""{a:1}""", """{}""")
  }
}
