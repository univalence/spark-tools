package io.univalence.sparktest

import org.apache.spark.sql._

trait SparkTest extends SparkTestSQLImplicits with SparkTest.ReadOps {

  /*object contains {
    def containsOnly(df: DataFrame, element: Any): Boolean    = ???
    def containsAtLeast(df: DataFrame, element: Any): Boolean = ???
    // df.columns.exists(name => df.filter(s"$name == '$element'").head(1).nonEmpty)
  }

  trait Comparisons {
    def assertEquality(df: DataFrame, expectedDF: DataFrame): Boolean
  }*/

  lazy val ss: SparkSession             = SparkTestSession.spark
  protected def _sqlContext: SQLContext = ss.sqlContext

  implicit class SparkTestDsOps[T: Encoder](_ds: Dataset[T]) {
    def shouldExists(pred: T => Boolean): Unit =
      if (!_ds.collect().exists(pred))
        throw new AssertionError("All the rows do not match the predicate.")

    def shouldForAll(pred: T => Boolean): Unit =
      if (!_ds.collect().forall(pred))
        throw new AssertionError("At least one row does not match the predicate.")

    def assertContains(values: T*): Unit = {
      val dsArray = _ds.collect()
      if (!values.forall(dsArray.contains(_)))
        throw new AssertionError("At least one value was not in the dataset.")
    }

    def assertEquals(ds: Dataset[T]): Unit = {
      if (_ds.schema != ds.schema)
        throw new AssertionError("The data set schema is different")
      else if (!_ds.collect().sameElements(ds.collect()))
        throw new AssertionError("The data set content is different")
    }

    def assertEquals(seq: Seq[T]): Unit = {
      val schema = _ds.schema
      val df = seq.toDF()
      val newDF = _sqlContext.createDataFrame(df.rdd, schema)
      assertEquals(newDF.as[T])
    }

  }

  implicit class SparkTestDfOps(df: DataFrame) {
    def assertEquals(otherDf: DataFrame): Unit =
      if (df.schema != otherDf.schema)
        throw new AssertionError("The data set schema is different")
      else if (!df.collect().sameElements(otherDf.collect()))
        throw new AssertionError("The data set content is different")

    def showCaseClass(className: String): Unit = {
      val s2cc = new Schema2CaseClass
      import s2cc.implicits._
      println(s2cc.schemaToCaseClass(df.schema, className))
    } //PrintCaseClass Definition from Dataframe inspection

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