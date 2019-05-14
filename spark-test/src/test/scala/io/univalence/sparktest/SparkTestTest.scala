package io.univalence.sparktest
import org.scalatest.FunSuite

//I would be nicer to extends the trait and have all the feature instead of importing things
class SparkTestTest extends FunSuite with SparkTest {

  test("load Json") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df = dfFromJsonFile(path)
    df.show()
  }

  test("contains at least") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df = dfFromJsonFile(path)

    val ageExpected = 30
    val wrongAgeExpected = 17
    val nameExpected = "Andy"

    assert(df.containsAtLeast(ageExpected))
    assert(!df.containsAtLeast(wrongAgeExpected))
    assert(df.containsAtLeast(nameExpected))
  }
}
