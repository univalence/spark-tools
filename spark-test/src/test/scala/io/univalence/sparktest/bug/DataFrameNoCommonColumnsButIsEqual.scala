package io.univalence.sparktest.bug

import io.univalence.schema.SchemaComparator.NoCommonFieldError
import io.univalence.sparktest.SparkTest
import org.scalatest.FunSuite

class DataFrameNoCommonColumnsButIsEqual extends FunSuite with SparkTest {
  test("with failOnMissingExpectedCol config") {
    val actualDf = dataframe("{c:0}")
    val expectedDf = dataframe("{a:0, b:false}")

    assertThrows[NoCommonFieldError.type] {
      withConfiguration(failOnMissingExpectedCol = false)(
        actualDf.assertEquals(expectedDf)
      )
    }
  }

  test("with failOnMissingExpectedCol config and empty dataframes") {
    val actualDf = dataframe("{}")
    val expectedDf = dataframe("{}")

    withConfiguration(failOnMissingExpectedCol = false)(
      actualDf.assertEquals(expectedDf)
    )
  }
}
