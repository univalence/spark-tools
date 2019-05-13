package io.univalence.sparktest

import org.apache.spark.sql.DataFrame


trait SparkTest {
  trait Read {
    def dfFromJson(path: String): DataFrame
  }

  trait Contains {
    def containsOnly(df: DataFrame): Boolean

    def containsAtLeast(df: DataFrame): Boolean
  }

  trait Comparisons {
    def assertEquality(df: DataFrame, expectedDF: DataFrame): Boolean
  }
}

object DFLoad extends SparkTest with SparkTestSession {
  def read: Read = new Read {
    override def dfFromJson(path: String): DataFrame = spark.read.json(path)
  }
}

object DFContentTest extends SparkTest {
  def contains: Contains = new Contains {
    override def containsOnly(df: DataFrame): Boolean = ???

    override def containsAtLeast(df: DataFrame): Boolean = ???
  }
}

object DFComparisons extends SparkTest with SparkTestSession {
  def comparisons: Comparisons = new Comparisons {
    override def assertEquality(df: DataFrame, expectedDF: DataFrame): Boolean = ???
  }
}