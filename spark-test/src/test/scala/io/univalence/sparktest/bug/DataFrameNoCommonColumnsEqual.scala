package io.univalence.sparktest.bug

import io.univalence.schema.SchemaComparator.SchemaError
import io.univalence.sparktest.SparkTest
import org.scalatest.FunSuite

class DataFrameNoCommonColumnsEqual extends FunSuite with SparkTest {
  ignore("with failOnMissingExpectedCol config") {
    val actualDf = dataframe("{c:0}")
    val expectedDf = dataframe("{a:0, b:false}")

    assertThrows[SchemaError] {
      withConfiguration(failOnMissingExpectedCol = false)(
        actualDf.assertEquals(expectedDf)
      )
    }
  }
}
