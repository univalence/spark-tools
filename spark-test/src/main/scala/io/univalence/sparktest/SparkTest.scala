package io.univalence.sparktest

import org.apache.spark.sql.DataFrame

trait SparkTest {
  trait Read {
    def dfFromJson(path: String): DataFrame
  }

  trait Contains {
    def containsOnly(df: DataFrame, element: Any): Boolean

    def containsAtLeast(df: DataFrame, element: Any): Boolean
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
    override def containsOnly(df: DataFrame, element: Any): Boolean = ???

    override def containsAtLeast(df: DataFrame, element: Any): Boolean =
      // TODO: element = Case class
      df.columns.exists(name => df.filter(s"$name == '$element'").head(1).nonEmpty)
  }

  implicit class ContainsOps (df: DataFrame) {
    def containsAtLeast(element: Any): Boolean = contains.containsAtLeast(df, element)
    def containsOnly(element: Any): Boolean = contains.containsOnly(df, element)
  }
}

object DFComparisons extends SparkTest with SparkTestSession {
  def comparisons: Comparisons = new Comparisons {
    override def assertEquality(df: DataFrame, expectedDF: DataFrame): Boolean = ???
  }
}