package io.univalence.schema

import io.univalence.sparktest.SparkTest
import org.scalatest.FunSuite
import io.univalence.typedpath._

class SchemaTest extends FunSuite with SparkTest {

  test("testMove") {

    val m = Schema.move(key"a", key"b").andThen(_.get)

    def check(in: String, out: String): Unit =
      m(dataframe(in)).assertEquals(dataframe(out))

    check("{a:1}", "{b:1}")
    check("{a:1, c:0}", "{b:1, c:0}")
  }

}
