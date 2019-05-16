package io.univalence.sparktest
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

//I would be nicer to extends the trait and have all the feature instead of importing things
class SparkTestTest extends FunSuite with SparkTest {

  test("load Json from String") {

    //manage json option for jackson
    val df = dfFromJsonString("{a:1}", "{a:2}")

    df.show()

    df.as[Long].assertEquals(Seq(1L, 2L))

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

  test("should assertEquals between equal DF") {
    val dfUT = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 2, 3).toDF("id")

    dfUT.assertEquals(dfExpected)
  }

  test("should assertEquals between equal DS") {
    // assertEquals(ds)
    val dsUT = Seq(1, 2, 3).toDS
    val dsExpected = Seq(1, 2, 3).toDS

    dsUT.assertEquals(dsExpected)

    // assertEquals(seq)
    val dsPerson = Seq(Person("John", 15), Person("Alicia", 18)).toDS
    val seqExpected = Seq(Person("John", 15), Person("Alicia", 18))

    dsPerson.assertEquals(seqExpected)
  }
}

case class Person(name: String, age: Int)