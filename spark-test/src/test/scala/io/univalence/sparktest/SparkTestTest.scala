package io.univalence.sparktest
import org.scalatest.FunSuiteLike

//I would be nicer to extends the trait and have all the feature instead of importing things
class SparkTestTest extends FunSuiteLike with SparkTest {

  ignore("load Json from String") {

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

  test("should not assertEquals between DF with different contents") {
    val dfUT = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(3, 2, 1).toDF("id")

    assertThrows[AssertionError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  test("should not assertEquals between DF with different schema") {
    val dfUT = Seq(1, 2, 3).toDF("id")
    val dfExpected = Seq(1, 2, 3).toDF("di")

    assertThrows[AssertionError] {
      dfUT.assertEquals(dfExpected)
    }
  }

  test("should exists if at least one row matches the predicate") {
    val ds = Seq(1, 2, 3).toDS()
    ds.shouldExists(i => i > 2)
  }

  test("should not exists if all the rows don't match the predicate") {
    val ds = Seq(1, 2, 3).toDS()
    assertThrows[AssertionError] {
      ds.shouldExists(i => i > 3)
    }
  }

  test("should not throw an exception if all the rows match the predicate") {
    val ds = Seq(1, 2, 3).toDS()
    ds.shouldForAll(i => i >= 1)
  }

  test("should throw an exception if one of the row does not match the predicate") {
    val ds = Seq(1, 2, 3).toDS()
    assertThrows[AssertionError] {
      ds.shouldForAll(i => i >= 2)
    }
  }

  test("should not throw an exception if the dataset contains all values") {
    val ds = Seq(1, 2, 3).toDS()
    ds.assertContains(1, 2)
  }

  test("should throw an exception if the dataset does not contain at least one value") {
    val ds = Seq(1, 2, 3).toDS()
    assertThrows[AssertionError] {
      ds.assertContains(1, 2, 4)
    }
  }

}
