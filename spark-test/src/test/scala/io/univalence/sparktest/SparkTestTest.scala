package io.univalence.sparktest
import org.scalatest.FunSuite

class SparkTestTest extends FunSuite {
  import io.univalence.sparktest.DFLoad._
  import io.univalence.sparktest.DFContentTest._

  test("load Json") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df = read.dfFromJson(path)
    df.show()
  }

  test("contains at least") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df = read.dfFromJson(path)

    val ageExpected = 30
    val wrongAgeExpected = 17
    val nameExpected = "Andy"

    assert(df.containsAtLeast(ageExpected))
    assert(!df.containsAtLeast(wrongAgeExpected))
    assert(df.containsAtLeast(nameExpected))
  }
}
