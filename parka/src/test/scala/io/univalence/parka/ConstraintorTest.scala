package io.univalence.parka

import io.univalence.parka.Constraintor.{Fail, Pass, colChange, colNotChange, colsChange, colsNotChange, isSimilar, noInner, noOuter}
import io.univalence.sparktest.SparkTest
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class ConstraintorTest extends FunSuite with SparkTest {
  test("constraintor should pass the similar test") {
    val data: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )

    val analysis            = Parka(data, data)("key");
    assert(Constraintor.respectConstraints(analysis)(noInner, noOuter) == Pass)
    assert(Constraintor.respectConstraints(analysis)(isSimilar) == Pass)
  }

  test("constraintor should not pass the similar test") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 3),
        Element("1", 2),
        Element("2", 1),
        Element("3", 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(isSimilar) == Fail(List(isSimilar): _*))
  }

  test("constraintor should pass the no outer test") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 3),
        Element("1", 2),
        Element("2", 1),
        Element("3", 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(noOuter) == Pass)
  }

  test("constraintor should not pass the no outer test") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 3),
        Element("1", 2),
        Element("2", 1),
        Element("3", 0),
        Element("4", 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(noOuter) == Fail(List(noOuter): _*))
  }

  test("constraintor should pass the no inner test") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3),
        Element("4", 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(noInner) == Pass)
  }

  test("constraintor should not pass the no inner test") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 3),
        Element("1", 2),
        Element("2", 1),
        Element("3", 0),
        Element("4", 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(noInner) == Fail(List(noInner): _*))
  }

  test("constraintor should fail the ColChange and pass the ColNotChange") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(colNotChange("col1")) == Pass)
    assert(Constraintor.respectConstraints(analysis)(colChange("col1")) == Fail(List(colChange("col1")): _*))
  }

  test("constraintor should fail the ColChange and pass the ColNotChange when another column changed") {
    val left: Dataset[Element2] =
      dataset(
        Element2("0", 0, 0),
        Element2("1", 1, 1),
        Element2("2", 2, 2),
        Element2("3", 3, 3)
      )
    val right: Dataset[Element2] =
      dataset(
        Element2("0", 0, 3),
        Element2("1", 1, 2),
        Element2("2", 2, 1),
        Element2("3", 3, 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(colNotChange("col1")) == Pass)
    assert(Constraintor.respectConstraints(analysis)(colChange("col1")) == Fail(List(colChange("col1")): _*))
  }

  test("constraintor should pass the ColChange and fail the ColNotChange") {
    val left: Dataset[Element] =
      dataset(
        Element("0", 0),
        Element("1", 1),
        Element("2", 2),
        Element("3", 3)
      )
    val right: Dataset[Element] =
      dataset(
        Element("0", 3),
        Element("1", 2),
        Element("2", 1),
        Element("3", 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(colChange("col1")) == Pass)
    assert(Constraintor.respectConstraints(analysis)(colNotChange("col1")) == Fail(List(colNotChange("col1")): _*))
  }

  test("constraintor should pass the ColsChange and fail the ColsNotChange") {
    val left: Dataset[Element2] =
      dataset(
        Element2("0", 3, 0),
        Element2("1", 2, 1),
        Element2("2", 1, 2),
        Element2("3", 0, 3)
      )
    val right: Dataset[Element2] =
      dataset(
        Element2("0", 0, 3),
        Element2("1", 1, 2),
        Element2("2", 2, 1),
        Element2("3", 3, 0)
      )

    val analysis            = Parka(left, right)("key");
    assert(Constraintor.respectConstraints(analysis)(colsChange("col1", "col2")) == Pass)
    assert(Constraintor.respectConstraints(analysis)(colChange("col1"), colChange("col2")) == Pass)
    assert(Constraintor.respectConstraints(analysis)(colsNotChange("col1", "col2")) == Fail(List(colsNotChange("col1","col2")): _*))
    assert(Constraintor.respectConstraints(analysis)(colNotChange("col1"), colNotChange("col2")) == Fail(List(colNotChange("col1"), colNotChange("col2")): _*))
  }
}