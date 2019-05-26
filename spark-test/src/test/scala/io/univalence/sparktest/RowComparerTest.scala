package io.univalence.sparktest

import io.univalence.sparktest.RowComparer.areRowsEqual
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.scalatest.{FunSuiteLike, Matchers}

class RowComparerTest extends FunSuiteLike with Matchers {

  test("empty rows should be equal") {
    val row1 = Row()
    val row2 = Row()

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with different length should not be equal") {
    val row1 = Row(1, 2)
    val row2 = Row(1, 2, 3)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with null and non-null value at the same index should not be equal") {
    val row1 = Row(null, 2)
    val row2 = Row(1, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with same byte arry should be equal") {
    val row1 = Row(Array[Byte](1.toByte))
    val row2 = Row(Array[Byte](1.toByte))

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("row with byte array and row with different byte array at the same index should not be equal") {
    val row1 = Row(Array[Byte](1.toByte), 2)
    val row2 = Row(Array[Byte](2.toByte), 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("row with NaN and row with a float at the same index should not be equal") {
    val row1 = Row(Float.NaN, 2)
    val row2 = Row(2.0f, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with float NaN at the same index should not be equal") {
    val row1 = Row(Float.NaN, 2)
    val row2 = Row(Float.NaN, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("row with NaN and row with a double at the same index should not be equal") {
    val row1 = Row(Double.NaN, 2)
    val row2 = Row(2.0, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with double NaN at the same index should not be equal") {
    val row1 = Row(Double.NaN, 2)
    val row2 = Row(Double.NaN, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with same float should be equal") {
    val row1 = Row(1.0f)
    val row2 = Row(1.0f)

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("row with float and row with different float at the same index should not be equal") {
    val row1 = Row(1.0f, 2)
    val row2 = Row(2.0f, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with same double should be equal") {
    val row1 = Row(1.0)
    val row2 = Row(1.0)

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("row with double and row with different double at the same index should not be equal") {
    val row1 = Row(1.0, 2)
    val row2 = Row(2.0, 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with same bigdecimal should be equal") {
    val row1 = Row(BigDecimal("1.0"))
    val row2 = Row(BigDecimal("1.0"))

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("row with bigdecimal and row with different bigdecimal at the same index should not be equal") {
    val row1 = Row(BigDecimal("1.0"), 2)
    val row2 = Row(BigDecimal("2.0"), 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with same timestamp should be equal") {
    val row1 = Row(new Timestamp(1L))
    val row2 = Row(new Timestamp(1L))

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("row with timestamp and row with different timestamp at the same index should not be equal") {
    val row1 = Row(new Timestamp(1L), 2)
    val row2 = Row(new Timestamp(2L), 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with same person should be equal") {
    val row1 = Row(RowComparerPerson("1", "toto", 32))
    val row2 = Row(RowComparerPerson("1", "toto", 32))

    assert(areRowsEqual(row1, row2, 1e-7))
  }

  test("rows with different persons at the same index should not be equal") {
    val row1 = Row(RowComparerPerson("1", "toto", 32), 2)
    val row2 = Row(RowComparerPerson("2", "tata", 32), 2)

    assert(!areRowsEqual(row1, row2, 1e-7))
  }

}

case class RowComparerPerson(id: String, name: String, age: Int)