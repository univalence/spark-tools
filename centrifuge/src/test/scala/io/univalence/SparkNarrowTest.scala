package io.univalence

import java.net.URLClassLoader
import java.sql.Date

import io.univalence.centrifuge.Sparknarrow
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

case class Person(name: String, age: Int, date: Date)

class SparknarrowTest extends FunSuite {

  val conf: SparkConf = new SparkConf()
  conf.setAppName("yo")
  conf.set("spark.sql.caseSensitive", "true")
  conf.setMaster("local[2]")

  implicit val ss: SparkSession = SparkSession.builder.config(conf).getOrCreate
  import ss.implicits._

  test("testBasicCC") {

    val classDef = Sparknarrow.basicCC(Encoders.product[Person].schema).classDef
    checkDefinition(classDef)

  }

  def checkDefinition(scalaCode: String): Unit = {
    //TODO do a version for 2.11 and 2.12
    /*
    import scala.tools.reflect.ToolBox
    import scala.reflect.runtime.{ universe => ru }
    import ru._

    val cl = this.getClass.getClassLoader

    val tb = ru.runtimeMirror(cl).mkToolBox()

    val parsed = tb.parse(scalaCode)
    tb.compile(parsed)()
   */
  }

  test("play with scala eval") {

    val code =
      """
        case class Tata(str: String)
        case class Toto(age: Int, tata: Tata)
      """

    checkDefinition(code)
    checkDefinition(code)

  }

  ignore("printSchema StructType") {
    val yo = StructType(
      Seq(
        StructField("name", StringType),
        StructField("tel", ArrayType(StringType))
      )
    )

    yo.printTreeString()
  }

}
