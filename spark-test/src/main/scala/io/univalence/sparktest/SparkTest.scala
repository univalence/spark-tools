package io.univalence.sparktest

import org.apache.spark.sql._

trait SparkTest extends SparkTestSQLImplicits with SparkTest.ReadOps {

  object contains {
    def containsOnly(df: DataFrame, element: Any): Boolean    = ???
    def containsAtLeast(df: DataFrame, element: Any): Boolean = ???
    // df.columns.exists(name => df.filter(s"$name == '$element'").head(1).nonEmpty)
  }

  trait Comparisons {
    def assertEquality(df: DataFrame, expectedDF: DataFrame): Boolean
  }

  lazy val ss: SparkSession             = SparkTestSession.spark
  protected def _sqlContext: SQLContext = ss.sqlContext

  implicit class SparkTestDsOps[T: Encoder](_ds: Dataset[T]) {
    def shouldExists(pred: T => Boolean): Unit = ??? //TODO throws exception if false
    def shouldForAll(pred: T => Boolean): Unit = ??? //TODO throws exception if false

    def assertContains(values: T*): Unit = ???

    def assertEquals(ds: Dataset[T]): Unit = ???

    def assertEquals(seq: Seq[T]): Unit = ???

  }

  implicit class SparkTestDfOps(df: DataFrame) {
    def assertEquals(otherDf: DataFrame): Unit = {
      df.schema == otherDf.schema && df.collect().sameElements(otherDf.collect())
    }

    def showCaseClass(): Unit = ??? //PrintCaseClass Definition from Dataframe inspection

    def containsAtLeast(element: Any): Boolean = contains.containsAtLeast(df, element)
    def containsOnly(element: Any): Boolean    = contains.containsOnly(df, element)
  }
}

object SparkTest {
  trait HasSparkSession {
    def ss: SparkSession
  }
  trait ReadOps extends HasSparkSession {
    def dfFromJsonString(json: String*): DataFrame = {
      val _ss = ss
      import _ss.implicits._

      ss.read.option("allowUnquotedFieldNames",value = true).json(ss.createDataset[String](json).rdd)
    }
    def dfFromJsonFile(path: String): DataFrame   = ss.read.json(path)
  }

}
