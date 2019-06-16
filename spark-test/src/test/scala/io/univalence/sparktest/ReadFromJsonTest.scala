package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ReadFromJsonTest extends FunSuite with SparkTest {
  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  test("load Json from String") {
    //manage json option for jackson
    val df = dataframe("{a:1}", "{a:2}")

    //df.as[Long].assertEquals(Seq(1L, 2L))

    df.as[Long].assertContains(1, 2)
    df.as[Long].assertContains(1)
    df.as[Long].assertContains(2)

    df.assertEquals(df)
  }

  ignore("load Json from String2") {
    //#Hard we need to use magnolia to solve this one and generate an expression encoder
    /*
    val df = dfFromJsonString("[{a:1},{a:2}]")

    case class A(a:Int)

    df.as[A].assertContains(A(1),A(2))
   */
  }

  /*
  TODO : manage spark logs, only warnings, no logs in info
  TODO : optimize spark for small load
   */

  test("load Json from file") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df   = dfFromJsonFile(path)

    assert(df.count == 3)
  }

  test("load Json from multiple files") {
    val path  = "spark-test/src/test/resources/jsonTest.json"
    val path2 = "spark-test/src/test/resources/jsonTest2.json"
    val df    = loadJson(path, path2)

    assert(df.count == 9)
  }
}
