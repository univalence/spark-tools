package io.univalence.sparktest
import org.scalatest.FunSuite

class SparkTestTest extends FunSuite {
  import io.univalence.sparktest.DFLoad._

  test("load Json") {
    val path = "spark-test/src/test/resources/jsonTest.json"
    val df = read.dfFromJson(path)
    df.show()
  }
}
