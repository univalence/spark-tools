package io.univalence.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag
import io.univalence.sparktest.RowComparer._
import io.univalence.sparktest.SchemaComparison.{ AddField, ChangeFieldType, RemoveField, SchemaModification }
import io.univalence.sparktest.ValueComparison.{
  compareValue,
  fromRow,
  toStringModifications,
  toStringRowsMods,
  ObjectModification
}
import io.univalence.sparktest.internal.DatasetUtils
import org.apache.spark.sql.types.StructType

import scala.util.Try

case class SparkTestConfiguration(failOnMissingOriginalCol: Boolean         = false,
                                  failOnChangedDataTypeExpectedCol: Boolean = true,
                                  failOnMissingExpectedCol: Boolean         = true)

trait SparkTest extends SparkTestSQLImplicits with SparkTest.ReadOps {

  private var configuration: SparkTestConfiguration = SparkTestConfiguration()

  def withConfiguration(
    failOnMissingOriginalCol: Boolean         = configuration.failOnMissingOriginalCol,
    failOnChangedDataTypeExpectedCol: Boolean = configuration.failOnChangedDataTypeExpectedCol,
    failOnMissingExpectedCol: Boolean         = configuration.failOnMissingExpectedCol
  )(body: => Unit): Unit = {
    val old = configuration
    configuration = configuration.copy(
      failOnMissingOriginalCol         = failOnMissingOriginalCol,
      failOnChangedDataTypeExpectedCol = failOnChangedDataTypeExpectedCol,
      failOnMissingExpectedCol         = failOnMissingExpectedCol
    )
    body
    configuration = old
  }

  lazy val ss: SparkSession = SparkTestSession.spark

  protected def _sqlContext: SQLContext = ss.sqlContext

  trait SparkTestError extends Exception

  case class SchemaError(modifications: Seq[SchemaModification]) extends SparkTestError {
    override def getMessage: String =
      modifications.foldLeft("") {
        case (msgs, error) =>
          error match {
            case SchemaModification(p, RemoveField(_)) =>
              if (configuration.failOnMissingOriginalCol)
                s"$msgs\nField ${p.firstName} was not in the expected DataFrame."
              else msgs
            case SchemaModification(p, ChangeFieldType(from, to)) =>
              if (configuration.failOnChangedDataTypeExpectedCol)
                s"$msgs\nField ${p.firstName} ($from) was not the same datatype as expected ($to)."
              else msgs
            case SchemaModification(p, AddField(_)) =>
              if (configuration.failOnMissingExpectedCol)
                s"$msgs\nField ${p.firstName} was not in the original DataFrame."
              else msgs
          }
      }
  }

  case class ValueError(modifications: Seq[Seq[ObjectModification]], thisDf: DataFrame, otherDf: DataFrame)
      extends SparkTestError {
    override def getMessage: String =
      thisDf.reportErrorComparison(otherDf, modifications)

  }

  // ========================== DATASET ====================================

  implicit class SparkTestDsOps[T: Encoder](thisDs: Dataset[T]) {

    DatasetUtils.cacheIfNotCached(thisDs)

    def shouldForAll(pred: T => Boolean): Unit =
      if (!thisDs.collect().forall(pred)) {
        val displayErr = thisDs.collect().filterNot(pred).take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }

    def shouldExists(pred: T => Boolean): Unit =
      if (!thisDs.collect().exists(pred)) {
        val displayErr = thisDs.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }

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
    
    def reduceColumn[B](otherDs: Dataset[B]): Try[(DataFrame, DataFrame)] =
      thisDs.toDF.reduceColumn(otherDs.toDF)

    // TODO : Faire sans les toDF !
    def assertEquals[B](otherDs: Dataset[B])(implicit encB: Encoder[B]): Unit = {
      // Compare Schema and try to reduce it
      val (reducedThisDf, reducedOtherDf) = thisDs.reduceColumn(otherDs).get

      if (!reducedThisDf.collect().sameElements(reducedOtherDf.collect())) {
        val valueMods = reducedThisDf.getRowsDifferences(reducedOtherDf)
        throw ValueError(valueMods, thisDs.toDF, otherDs.toDF)
      }
    }

    def assertEquals(seq: Seq[T]): Unit = {
      val dfFromSeq = ss.createDataFrame(ss.sparkContext.parallelize(seq.map(Row(_))), thisDs.schema)
      assertEquals(dfFromSeq.as[T])
    }

    def assertApproxEquals(otherDs: Dataset[T], approx: Double, ignoreNullableFlag: Boolean = false): Unit = {
      val rows1  = thisDs.toDF.collect()
      val rows2  = otherDs.toDF.collect()
      val zipped = rows1.zip(rows2)
      if (SchemaComparison.compareSchema(thisDs.schema, otherDs.schema).nonEmpty)
        throw new AssertionError(
          s"The data set schema is different:\n${SparkTest.displayErrSchema(thisDs.schema, otherDs.schema)}"
        )
      zipped.foreach {
        case (r1, r2) =>
          if (!areRowsEqual(r1, r2, approx))
            throw new AssertionError(s"$r1 was not equal approx to expected $r2, with a $approx approx")
      }
    }
  }

  // ========================== DATAFRAME ====================================

  implicit class SparkTestDfOps(thisDf: DataFrame) {
    DatasetUtils.cacheIfNotCached(thisDf)

    /**
      * Depending on the configuration, try to modify the schema of the two DataFrames so that they can be compared.
      * Throw a SchemaError otherwise.
      * @param otherDf
      * @return
      */
    def reduceColumn(otherDf: DataFrame): Try[(DataFrame, DataFrame)] = {
      val modifications = SchemaComparison.compareSchema(thisDf.schema, otherDf.schema)
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
            }
        }
      } else {
        (thisDf, otherDf)
      })
    }

    def assertEquals(otherDf: DataFrame): Unit = {
      // Compare Schema and try to reduce it
      val (reducedThisDf, reducedOtherDf) = thisDf.reduceColumn(otherDf).get

      if (!reducedThisDf.collect().sameElements(reducedOtherDf.collect())) {
        val valueMods = reducedThisDf.getRowsDifferences(reducedOtherDf)
        throw ValueError(valueMods, thisDf, otherDf)
      }
    }

    def assertEquals[T: Encoder](seq: Seq[T]): Unit = {
      val dfFromSeq = ss.createDataFrame(ss.sparkContext.parallelize(seq.map(Row(_))), thisDf.schema)
      assertEquals(dfFromSeq)
    }

    def getRowsDifferences(otherDf: DataFrame): Seq[Seq[ObjectModification]] = {
      val rows1 = thisDf.collect()
      val rows2 = otherDf.collect()

      rows1.zipAll(rows2, null, null).foldLeft(Seq(): Seq[Seq[ObjectModification]]) {
        case (acc, (curr1, curr2)) =>
          acc :+ compareValue(fromRow(curr1), fromRow(curr2))
      }
    }

    def reportErrorComparison(otherDf: DataFrame, modifications: Seq[Seq[ObjectModification]]): String = {
      val rowsDF1 = thisDf.collect()
      val rowsDF2 = otherDf.collect()

      val rows = for {
        rowModifications <- modifications.zipWithIndex
        diffs = rowModifications._1
        index = rowModifications._2
        if diffs.nonEmpty
      } yield {
        toStringModifications(diffs) ++ toStringRowsMods(diffs, rowsDF1(index), rowsDF2(index))
      }

      s"The data set content is different :\n\n${rows.take(10).mkString("\n\n")}\n"
    }

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
    }

    /**
      * Approximate comparison between two DataFrames.
      * If the DataFrames match the instances of Double, Float, or Timestamp, 'approx' will be used to compare an
      * approximate equality of the two DFs.
      *
      * @param otherDf            DataFrame to compare to
      * @param approx             double for the approximate comparison. If the absolute value of the difference between
      *                           two values is less than approx, then the two values are considered equal.
      * @param ignoreNullableFlag flag to decide if nullable is ignored or not
      */
    def assertApproxEquals(otherDf: DataFrame, approx: Double, ignoreNullableFlag: Boolean = false): Unit = {
      val rows1  = thisDf.collect()
      val rows2  = otherDf.collect()
      val zipped = rows1.zip(rows2)
      if (SchemaComparison
            .compareSchema(thisDf.schema, otherDf.schema)
            .nonEmpty) //(SparkTest.compareSchema(thisDf.schema, otherDf.schema, ignoreNullableFlag))
        throw new AssertionError(
          s"The data set schema is different\n${SparkTest.displayErrSchema(thisDf.schema, otherDf.schema)}"
        )
      zipped.foreach {
        case (r1, r2) =>
          if (!areRowsEqual(r1, r2, approx))
            throw new AssertionError(s"$r1 was not equal approx to expected $r2, with a $approx approx")
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
      * @param className name of the case class
      */
    def showCaseClass(className: String): Unit = {
      val s2cc = new Schema2CaseClass
      import s2cc.implicits._

      println(s2cc.schemaToCaseClass(thisDf.schema, className))
    } //PrintCaseClass Definition from Dataframe inspection

  }

  // ========================== RDD ====================================

  implicit class SparkTestRDDOps[T: ClassTag](thisRdd: RDD[T]) {
    def shouldForAll(f: T => Boolean): Unit = {
      val count: Long = thisRdd.map(v => f(v)).filter(_ == true).count()
      if (thisRdd.count() != count) { // !thisRdd.collect().forall(f)
        val displayErr = thisRdd.collect().filterNot(f).take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }

    def shouldExists(f: T => Boolean): Unit = {
      val empty = thisRdd.map(v => f(v)).filter(_ == true).isEmpty()
      if (empty) { // !thisRdd.collect().exists(f)
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

      //TODO : ReduceByKey + Cogroup give us 5 stages, we have to do the cogroup directly to have only 3 stages
      // and ReduceByKey can be done in the mapping post cogroup : DONE ?

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
      // Group them together and filter for difference
      /*
      expectedKeyed
        .reduceByKey(_ + _)
        .cogroup(resultKeyed.reduceByKey(_ + _))
        .filter {
          case (_, (i1, i2)) =>
            i1.isEmpty || i2.isEmpty || i1.head != i2.head
        }
        .take(1)
        .headOption
        .map {
          case (v, (i1, i2)) =>
            (v, i1.headOption.getOrElse(0), i2.headOption.getOrElse(0))
        }

     */
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

  def setAllFieldsNullable(sc: StructType): StructType =
    StructType(sc.map(sf => sf.copy(nullable = true)))

  //Comparaison de Row
  case class RowDiff(label: String, left: Any, right: Any) {
    assert(left != right)
  }

  def compareRow(r1: Row, r2: Row, schema: StructType): Seq[RowDiff] =
    ???

  def modifyRow(r: Row, rowDiff: RowDiff): Try[Row] = ???

  //Comparaison de schema

  def compareSchema(sc1: StructType, sc2: StructType, ignoreNullableFlag: Boolean): Boolean =
    if (ignoreNullableFlag) {
      val newSc1 = setAllFieldsNullable(sc1)
      val newSc2 = setAllFieldsNullable(sc2)
      newSc1 != newSc2
    } else {
      sc1 != sc2
    }

  // TODO : To delete
  def displayErrSchema(actualSt: StructType, expectedSt: StructType): String = {
    val errors = expectedSt.zip(actualSt).filter(x => x._1 != x._2)

    errors.map(diff => s"${diff._1} was not equal to ${diff._2}").mkString("\n")
  }

  trait HasSparkSession {
    def ss: SparkSession
  }

  trait ReadOps extends HasSparkSession {

    def dataframe(json: String*): DataFrame = {
      assert(json.nonEmpty)
      val _ss = ss
      import _ss.implicits._

      ss.read.option("allowUnquotedFieldNames", value = true).json(ss.createDataset[String](json).rdd)
    }

    def dfFromJsonFile(path: String): DataFrame = ss.read.json(path)

    def dataset[T](value: T*): Dataset[T] = {
      assert(value.nonEmpty)
      ???
    }

    def loadJson(filenames: String*): DataFrame = {
      assert(filenames.nonEmpty)
      ???
    }

  }

}
