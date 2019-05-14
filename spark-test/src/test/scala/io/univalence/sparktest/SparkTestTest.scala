package io.univalence.sparktest
import org.scalatest.FunSuite

//I would be nicer to extends the trait and have all the feature instead of importing things
class SparkTestTest extends FunSuite with SparkTest {

  ignore("load Json from String") {

    //manage json option for jackson
    val df = dfFromJsonString("[{a:1},{a:2}]")

    df.as[Int].assertEquals(Seq(1, 2))

    df.as[Int].assertContains(1, 2)
    df.as[Int].assertContains(1)
    df.as[Int].assertContains(2)

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

  test("load Json") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df   = dfFromJsonFile(path)
    assert(df.count == 3)
  }

  ignore("contains at least") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df   = dfFromJsonFile(path)

    val ageExpected      = 30
    val wrongAgeExpected = 17
    val nameExpected     = "Andy"

    assert(df.containsAtLeast(ageExpected))
    assert(!df.containsAtLeast(wrongAgeExpected))
    assert(df.containsAtLeast(nameExpected))
  }
}
