package io.univalence.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder




trait SparkTest extends SparkTestSQLImplicits with SparkTest.ReadOps {

  object contains {
    def containsOnly(df: DataFrame, element: Any): Boolean = ???
    def containsAtLeast(df: DataFrame, element: Any): Boolean = ???
    // df.columns.exists(name => df.filter(s"$name == '$element'").head(1).nonEmpty)
  }

  trait Comparisons {
    def assertEquality(df: DataFrame, expectedDF: DataFrame): Boolean
  }

  lazy val ss: SparkSession = SparkTestSession.spark
  protected def _sqlContext: SQLContext = ss.sqlContext

  implicit class SparkTestDsOps[T:Encoder](_ds:Dataset[T]) {
    def shouldExists(pred:T => Boolean):Unit = ??? //TODO throws exception if false
    def shouldForAll(pred:T => Boolean):Unit = ??? //TODO throws exception if false

    def assertContains(values:T*):Unit = ???

    def assertEquals(ds:Dataset[T]):Unit = ???

    def assertEquals(seq:Seq[T]):Unit = ???

  }

  implicit class SparkTestDfOps(df: DataFrame) {
    def showCaseClass():Unit = ??? //PrintCaseClass Definition from Dataframe inspection

    def containsAtLeast(element: Any): Boolean = contains.containsAtLeast(df, element)
    def containsOnly(element: Any): Boolean = contains.containsOnly(df, element)
  }
}

object SparkTest {
  trait HasSparkSession {
    def ss:SparkSession
  }
  trait ReadOps extends HasSparkSession {
    def dfFromJsonString(json:String):DataFrame = ???
    def dfFromJsonFile(path: String): DataFrame = ss.read.json(path)
  }

}