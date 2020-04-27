package io.univalence.sparktest

import io.univalence.schema.SchemaComparator
import io.univalence.schema.SchemaComparator.{
  AddField,
  ChangeFieldType,
  RemoveField,
  SchemaError,
  SchemaModification,
  SetNonNullable,
  SetNullable
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag
import io.univalence.sparktest.ValueComparison.{
  compareValue,
  fromRow,
  toStringModifications,
  toStringRowsMods,
  ObjectModification
}
import io.univalence.sparktest.internal.DatasetUtils
import org.apache.spark.sql.types.{ StructField, StructType }

import scala.util.Try

case class SparkTestConfiguration(failOnMissingOriginalCol: Boolean         = false,
                                  failOnChangedDataTypeExpectedCol: Boolean = true,
                                  failOnMissingExpectedCol: Boolean         = true,
                                  failOnNullable: Boolean                   = false,
                                  maxRowError: Int                          = -1)

trait SparkTest extends SparkTestSQLImplicits with SparkTest.ReadOps {

  private var configuration: SparkTestConfiguration = SparkTestConfiguration()

  /**
    * You can wrap your Spark-Test's blocks using a different configuration to customize the behaviour of the functions.
    *
    * Example:
    * withConfiguration(failOnMissingExpectedCol = false, failOnMissingOriginalCol = false)({ df1.assertEquals(df2) })
    * result: ignore if there are any extra columns in df1 or df2
    *
    * failOnMissingOriginalCol:         if true, Return an exception if a column appear in the original DataFrame
    *                                   but not in the expected one
    * failOnChangedDataTypeExpectedCol: if true, Return an exception if both columns don't have the same DataType
    * failOnMissingExpectedCol:         if true, Return an exception if a column appear in the expected DataFrame
    *                                   but not in the original one
    * maxRowError:                      if > 0, print maxRowError's rows during the error handling else print
    *                                   every row's errors
    */
  def withConfiguration(
    failOnMissingOriginalCol: Boolean         = configuration.failOnMissingOriginalCol,
    failOnChangedDataTypeExpectedCol: Boolean = configuration.failOnChangedDataTypeExpectedCol,
    failOnMissingExpectedCol: Boolean         = configuration.failOnMissingExpectedCol,
    failOnNullable: Boolean                   = configuration.failOnNullable,
    maxRowError: Int                          = configuration.maxRowError
  )(body: => Unit): Unit = {
    val old = configuration
    configuration = configuration.copy(
      failOnMissingOriginalCol         = failOnMissingOriginalCol,
      failOnChangedDataTypeExpectedCol = failOnChangedDataTypeExpectedCol,
      failOnMissingExpectedCol         = failOnMissingExpectedCol,
      failOnNullable                   = failOnNullable,
      maxRowError                      = maxRowError
    )
    body
    configuration = old
  }

  lazy val ss: SparkSession = SparkTestSession.spark

  protected def _sqlContext: SQLContext = ss.sqlContext

  sealed trait SparkTestError extends Exception

  case class ValueError(modifications: Seq[Seq[ObjectModification]], thisDf: DataFrame, otherDf: DataFrame)
      extends SparkTestError {
    override lazy val getMessage: String =
      thisDf.reportErrorComparison(otherDf, modifications)

  }

  case class ShouldError(thisDf: DataFrame) extends SparkTestError {
    override lazy val getMessage: String =
      s"Rows not matching the predicate : ${thisDf.take(10).mkString("\n")}"
  }

  // ========================== DATASET =====================================

  implicit class SparkTestDsOps[T: Encoder](thisDs: Dataset[T]) {

    DatasetUtils.cacheIfNotCached(thisDs)

    /**
      * Check if all the rows from the Dataset match the predicate. If not, return a SparkTestError.
      *
      * @param pred   predicate to match
      */
    def shouldForAll(pred: T => Boolean): Unit =
      if (thisDs.filter((x: T) => !pred(x)).take(1).nonEmpty) {
        throw ShouldError(thisDs.toDF)
      }

    /**
      * Check if at least one row from the Dataset match the predicate. If not, return a SparkTestError.
      *
      * @param pred   predicate to match
      */
    def shouldExist(pred: T => Boolean): Unit =
      if (thisDs.filter(pred).take(1).isEmpty) {
        throw ShouldError(thisDs.toDF)
      }

    /**
      * Check if all the rows from the Dataset match the sql expression. If not, return a SparkTestError.
      *
      * @param expr   expression to match
      */
    def shouldForAll(expr: String): Unit = {
      val col: Column = org.apache.spark.sql.functions.not(org.apache.spark.sql.functions.expr(expr))
      if (thisDs.filter(col).take(1).nonEmpty) {
        throw ShouldError(thisDs.toDF)
      }
    }

    /**
      * Check if at least one row from the Dataset match the sql expression. If not, return a SparkTestError.
      *
      * @param expr   expression to match
      */
    def shouldExist(expr: String): Unit = {
      val col: Column = org.apache.spark.sql.functions.expr(expr)
      if (thisDs.filter(col).take(1).isEmpty) {
        throw ShouldError(thisDs.toDF)
      }
    }

    /**
      * Check if all values are present into the dataset. If not, return a AssertionError.
      *
      * @param values   values that should be in the dataset
      */
    def assertContains(values: T*): Unit = {
      val dsArray = thisDs.collect()
      if (!values.forall(dsArray.contains(_))) {
        val displayErr = values.diff(dsArray).take(10)
        throw new AssertionError(
          "At least one value was not in the dataset. " +
            s"Values not in the dataset :\n${displayErr.mkString("\n")}"
        )
      }
    }

    // TODO : Faire sans les toDF !
    /**
      * Comparison between two Datasets, can be customized using the Spark-Test configuration
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol,
      *                       maxRowError
      * @param otherDs        Dataset to compare to
      * @return               an exception if both Datasets are not equals
      */
    def assertEquals[B](otherDs: Dataset[B])(implicit encB: Encoder[B]): Unit =
      thisDs.toDF.assertEquals(otherDs.toDF)

    /**
      * Verify if the Dataset and Sequence are equals, can be customized using the Spark-Test configuration
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol,
      *                       maxRowError
      * @param seq            Sequence to compare to
      * @return               an exception if the Dataset and the Sequence are not equals
      */
    def assertEquals(seq: Seq[T]): Unit = {
      val dfFromSeq = ss.createDataFrame(ss.sparkContext.parallelize(seq.map(Row(_))), thisDs.schema)
      assertEquals(dfFromSeq.as[T])
    }

    /**
      * Approximate comparison between two Datasets, can be customized using the Spark-Test configuration
      * If the Datasets match the instances of Double, Float, or Timestamp, 'approx' will be used to compare an
      * approximate equality of the two DSs.
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol,
      *                       maxRowError
      * @param otherDs        Dataset to compare to
      * @param approx         double for the approximate comparison. If the absolute value of the difference between
      *                       two values is less than approx, then the two values are considered equal.
      */
    def assertApproxEquals(otherDs: Dataset[T], approx: Double): Unit =
      thisDs.toDF.assertApproxEquals(otherDs.toDF, approx)
  }

  // ========================== DATAFRAME ====================================

  implicit class SparkTestDfOps(thisDf: DataFrame) {
    DatasetUtils.cacheIfNotCached(thisDf)

    /**
      * Depending on the configuration, try to modify the schema of the two DataFrames so that they can be compared.
      * Throw a SchemaError otherwise.
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol
      * @param otherDf        DataFrame to compare to
      * @return               both reduced DataFrames
      */
    def reduceColumn(otherDf: DataFrame): Try[(DataFrame, DataFrame)] = {
      val modifications = SchemaComparator.compareSchema(thisDf.schema, otherDf.schema)

      Try(if (modifications.nonEmpty) {
        modifications.foldLeft((thisDf, otherDf)) {
          case ((df1, df2), sm) =>
            sm match {
              case SchemaModification(p, RemoveField(_)) =>
                if (!configuration.failOnMissingOriginalCol)
                  (df1.drop(p.firstName), df2)
                else throw SchemaError(modifications)
              case SchemaModification(p, ChangeFieldType(_, to)) =>
                if (!configuration.failOnChangedDataTypeExpectedCol)
                  (df1.withColumn(p.firstName, df1.col(p.firstName).cast(to)), df2)
                else throw SchemaError(modifications)
              case SchemaModification(p, AddField(_)) =>
                if (!configuration.failOnMissingExpectedCol)
                  (df1, df2.drop(p.firstName))
                else throw SchemaError(modifications)
              case SchemaModification(p, SetNullable) =>
                if (!configuration.failOnNullable)
                  (df1, SparkTest.setNullable(df2, p.firstName, nullable = true))
                else throw SchemaError(modifications)
              case SchemaModification(p, SetNonNullable) =>
                if (!configuration.failOnNullable)
                  (df1, SparkTest.setNullable(df2, p.firstName, nullable = false))
                else throw SchemaError(modifications)

            }
        }
      } else {
        (thisDf, otherDf)
      })
    }

    /**
      * Comparison between two DataFrames, can be customized using the Spark-Test configuration
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol,
      *                       maxRowError
      * @param otherDf        DataFrame to compare to
      * @return               an exception if both DataFrames are not equals
      */
    def assertEquals(otherDf: DataFrame): Unit = {
      val (reducedThisDf, reducedOtherDf) = thisDf.reduceColumn(otherDf).get
      val (newThisDf, newOtherDf) = if (!reducedThisDf.columns.sameElements(reducedOtherDf.columns)) {
        import SparkTest.orderedColumnsDf
        (orderedColumnsDf(reducedThisDf), orderedColumnsDf(reducedOtherDf))
      } else (reducedThisDf, reducedOtherDf)
      if (!newThisDf.collect().sameElements(newOtherDf.collect())) {
        val valueMods = newThisDf.getRowsDifferences(newOtherDf)
        if (!valueMods.forall(_.isEmpty)) {
          throw ValueError(valueMods, thisDf, otherDf)
        }
      }
    }

    /**
      * Approximate comparison between two DataFrames, can be customized using the Spark-Test configuration
      * If the DataFrames match the instances of Double, Float, or Timestamp, 'approx' will be used to compare an
      * approximate equality of the two DFs.
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol,
      *                       maxRowError
      * @param otherDf        DataFrame to compare to
      * @param approx         double for the approximate comparison. If the absolute value of the difference between
      *                       two values is less than approx, then the two values are considered equal.
      */
    def assertApproxEquals(otherDf: DataFrame, approx: Double): Unit = {
      val (reducedThisDf, reducedOtherDf) = thisDf.reduceColumn(otherDf).get
      val valueMods                       = reducedThisDf.getRowsDifferences(reducedOtherDf, approx)
      if (valueMods.exists(_.nonEmpty)) {
        throw ValueError(valueMods, thisDf, otherDf)
      }
    }

    /**
      * Verify if the DataFrame and Sequence are equals, can be customized using the Spark-Test configuration
      *
      * @configuration        failOnMissingOriginalCol, failOnChangedDataTypeExpectedCol, failOnMissingExpectedCol,
      *                       maxRowError
      * @param seq            Sequence to compare to
      * @return               an exception if the DataFrame and the Sequence are not equals
      */
    def assertEquals[T: Encoder](seq: Seq[T]): Unit = {
      val dfFromSeq = ss.createDataFrame(ss.sparkContext.parallelize(seq.map(Row(_))), thisDf.schema)
      assertEquals(dfFromSeq)
    }

    // EN CONSTRUCTION
    def mapToDf(maps: Seq[Map[String, Any]]): DataFrame = {
      val _ss  = ss
      val cols = maps.flatMap(_.keys).distinct
      val values = maps.map(m => {
        val row = cols.map(k => {
          val value = m.getOrElse(k, null)
          value match {
            case null => null
            case _    => value.toString
          }
        })
        // Use shapeless here
        row match {
          case List(a, b, c, d, _*) => (a, b, c, d)
        }
      })
      val rdd = _ss.sparkContext.parallelize(values)
      _ss.createDataFrame(rdd).toDF(cols: _*)
    }

    /**
      * Return each modifications for each row between this DataFrame and otherDf
      *
      * @param otherDf          DataFrame to compare to
      * @return                 return the Sequence of modifications for each rows
      */
    def getRowsDifferences(otherDf: DataFrame, approx: Double = 0): Seq[Seq[ObjectModification]] = {
      val rows1 = thisDf.collect()
      val rows2 = otherDf.collect()

      rows1.zipAll(rows2, null, null).view.map(r => compareValue(fromRow(r._1), fromRow(r._2), approx))
    }

    /**
      * Return a stringified version of the first modifications between this DataFrame and otherDf
      * The number of showed modifications can be modified using the Spark-Test configuration's value: maxRowError
      *
      * @configuration          maxRowError
      * @param otherDf          DataFrame to compare to
      * @param modifications    Sequence of modifications for each rows
      * @return                 Stringified version of modifications
      */
    def reportErrorComparison(otherDf: DataFrame, modifications: Seq[Seq[ObjectModification]]): String = {
      val rowsDF1 = thisDf.collect()
      val rowsDF2 = otherDf.collect()

      def stringify(sequence: (Seq[ObjectModification], Int)): String = {
        val diffs = sequence._1
        val index = sequence._2
        toStringModifications(diffs) ++ toStringRowsMods(diffs, rowsDF1(index), rowsDF2(index))
      }

      if (configuration.maxRowError > 0) {
        val rows = modifications.view.zipWithIndex
          .filter(_._1.nonEmpty)
          .map(stringify)
          .take(configuration.maxRowError)
          .mkString("\n\n")
        s"The data set content is different :\n\n$rows\n"
      } else {
        val rows = modifications.zipWithIndex
          .filter(_._1.nonEmpty)
          .map(stringify)
          .mkString("\n\n")
        s"The data set content is different :\n\n$rows\n"
      }
    }

    /**
      * Display the schema of the DataFrame as a case class.
      * Example :
      * val df = Seq(1, 2, 3).toDF("id")
      *   df.showCaseClass("Id")
      *
      * result :
      * case class Id (
      * id:Int
      * )
      *
      * @param className  name of the case class
      */
    def showCaseClass(className: String): Unit = {
      val s2cc = new Schema2CaseClass
      import s2cc.implicits._

      println(s2cc.schemaToCaseClass(thisDf.schema, className))
    }

    // ========================== SHOULD REMOVE ?? ====================================
    def assertColumnEquality(rightLabel: String, leftLabel: String): Unit =
      if (compareColumn(rightLabel, leftLabel))
        throw new AssertionError("Columns are different")

    def compareColumn(rightLabel: String, leftLabel: String): Boolean = {
      val elements =
        thisDf
          .select(
            rightLabel,
            leftLabel
          )
          .collect()

      elements.exists(r => r(0) != r(1))
    } //PrintCaseClass Definition from DataFrame inspection
  }

  // ========================== RDD ====================================

  implicit class SparkTestRDDOps[T: ClassTag](thisRdd: RDD[T]) {
    def shouldForAll(f: T => Boolean): Unit = {
      val count: Long = thisRdd.map(v => f(v)).filter(_ == true).count()
      if (thisRdd.count() != count) {
        val displayErr = thisRdd.collect().filterNot(f).take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }

    def shouldExists(f: T => Boolean): Unit = {
      val empty = thisRdd.map(v => f(v)).filter(_ == true).isEmpty()
      if (empty) {
        val displayErr = thisRdd.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }

    def assertEquals(otherRdd: RDD[T]): Unit =
      if (compareRDD(otherRdd).isDefined)
        throw new AssertionError("The RDD is different")

    def assertEquals(seq: Seq[T]): Unit = {
      val seqRDD = ss.sparkContext.parallelize(seq)
      assertEquals(seqRDD)
    }

    def assertEquals(l: List[T]): Unit = {
      val listRDD = ss.sparkContext.parallelize(l)
      assertEquals(listRDD)
    }

    def compareRDD(otherRdd: RDD[T]): Option[(T, (Int, Int))] = {
      //
      val expectedKeyed: RDD[(T, Int)] = thisRdd.map(_  -> 1)
      val resultKeyed: RDD[(T, Int)]   = otherRdd.map(_ -> 1)

      expectedKeyed
        .cogroup(resultKeyed)
        .map {
          case (k, (b1, b2)) =>
            (k, (b1.size, b2.size))
        }
        .filter {
          case (_, (i1, i2)) =>
            i1 == 0 || i2 == 0 || i1 != i2
        }
        .take(1)
        .headOption
    }

    def assertEqualsWithOrder(otherRdd: RDD[T]): Unit =
      if (compareRDDWithOrder(otherRdd).isDefined)
        throw new AssertionError("The RDD is different")

    def compareRDDWithOrder(otherRdd: RDD[T]): Option[(Option[T], Option[T])] =
      // If there is a known partitioner just zip
      if (otherRdd.partitioner.nonEmpty && otherRdd.partitioner.contains(otherRdd.partitioner.get)) {
        otherRdd.compareRDDWithOrderSamePartitioner(otherRdd)
      } else {
        // Otherwise index every element
        def indexRDD(rdd: RDD[T]): RDD[(Long, T)] =
          rdd.zipWithIndex.map { case (x, y) => (y, x) }

        val indexedExpected = indexRDD(thisRdd)
        val indexedResult   = indexRDD(otherRdd)

        indexedExpected
          .cogroup(indexedResult)
          .filter {
            case (_, (i1, i2)) =>
              i1.isEmpty || i2.isEmpty || i1.head != i2.head
          }
          .take(1)
          .headOption
          .map {
            case (_, (i1, i2)) =>
              (i1.headOption, i2.headOption)
          }
          .take(1)
          .headOption
      }

    /**
      * Compare two RDDs. If they are equal returns None, otherwise
      * returns Some with the first mismatch. Assumes we have the same partitioner.
      */
    // Source code :
    // https://github.com/holdenk/spark-testing-base/blob/master/core/src/main/1.3/scala/com/holdenkarau/spark/testing/RDDComparisons.scala
    def compareRDDWithOrderSamePartitioner(otherRdd: RDD[T]): Option[(Option[T], Option[T])] =
      // Handle mismatched lengths by converting into options and padding with Nones
      thisRdd
        .zipPartitions(otherRdd) { (thisIter, otherIter) =>
          new Iterator[(Option[T], Option[T])] {
            def hasNext: Boolean = thisIter.hasNext || otherIter.hasNext

            def next(): (Option[T], Option[T]) =
              (thisIter.hasNext, otherIter.hasNext) match {
                case (false, true) => (Option.empty[T], Some(otherIter.next()))
                case (true, false) => (Some(thisIter.next()), Option.empty[T])
                case (true, true)  => (Some(thisIter.next()), Some(otherIter.next()))
                case _             => throw new Exception("next called when elements consumed")
              }
          }
        }
        .filter { case (v1, v2) => v1 != v2 }
        .take(1)
        .headOption
  }

}

object SparkTest {
  sealed trait HasSparkSession {
    def ss: SparkSession
  }

  sealed trait ReadOps extends HasSparkSession {

    /**
      * Create a dataframe using a json
      *
      * Example:
      * val df = dataframe("{a:1, b:true}", "{a:2, b:false}")
      * | a |   b   |
      * +---+-------+
      * | 1 | true  |
      * | 2 | false |
      *
      * @param json       json's agruments, each argument represent one line of our dataframe
      * @return           a dataframe
      */
    def dataframe(json: String*): DataFrame = {
      assert(json.nonEmpty)
      val _ss = ss
      import _ss.implicits._

      ss.read.option("allowUnquotedFieldNames", value = true).json(ss.createDataset[String](json).rdd)
    }

    //Todo: Add an example
    /**
      * Create a dataframe using a json file
      *
      * Example:
      *
      * @param path       The path where the json is stored
      * @return           The resulting DataFrame
      */
    def dfFromJsonFile(path: String): DataFrame = ss.read.json(path)

    def dataset[T: Encoder: ClassTag](value: T*): Dataset[T] = {
      assert(value.nonEmpty)
      val _ss = ss
      import _ss.implicits._
      ss.sparkContext.parallelize(value, 1).toDS
    }

    def loadJson(filenames: String*): DataFrame = {
      assert(filenames.nonEmpty)
      ss.read.json(filenames: _*)
    }

  }

  /**
    * Set the field's nullable property to nullable
    *
    * @param df             the DataFrame
    * @param field          the column name
    * @param nullable       the new nullable's value
    * @return               the resulting DataFrame
    */
  private def setNullable(df: DataFrame, field: String, nullable: Boolean): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(f, t, n, m) if f.equals(field) && n != nullable => StructField(f, t, !n, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  private def orderedColumnsDf(df: DataFrame): DataFrame = {
    val sortedColumns = df.columns.sorted
    df.select(sortedColumns.head, sortedColumns.tail: _*)
  }

}
