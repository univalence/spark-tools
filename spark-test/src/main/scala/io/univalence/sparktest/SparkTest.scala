package io.univalence.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag
import io.univalence.sparktest.RowComparer._
import org.apache.spark.sql.types.StructType

trait SparkTest extends SparkTestSQLImplicits with SparkTest.ReadOps {

  lazy val ss: SparkSession = SparkTestSession.spark

  protected def _sqlContext: SQLContext = ss.sqlContext

  // ========================== DATASET ====================================

  implicit class SparkTestDsOps[T: Encoder](thisDs: Dataset[T]) {
    thisDs.cache()

    /*def shouldForAll(f: T => Boolean): Unit = {
      val count = thisDs.map(v => f(v)).filter(_ == true).count()
      if (thisDs.count() != count) { // !thisRdd.collect().forall(pred)
        val displayErr = thisDs.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }

    def shouldExists(f: T => Boolean): Unit = {
      val size = thisDs.map(v => f(v)).filter(_ == true).count()
      if (size == 0) { // !thisRdd.collect().exists(pred)
        val displayErr = thisDs.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }*/

    def shouldForAll(pred: T => Boolean): Unit = {
      if (!thisDs.collect().forall(pred)) {
        val displayErr = thisDs.collect().filterNot(pred).take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }

    def shouldExists(pred: T => Boolean): Unit = {
      if (!thisDs.collect().exists(pred)) {
        val displayErr = thisDs.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
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

    def assertEquals(otherDs: Dataset[T],
                     checkRowOrder: Boolean = false,
                     ignoreNullableFlag: Boolean = false,
                     ignoreSchemaFlag: Boolean = false): Unit =
      if (!ignoreSchemaFlag && SparkTest.compareSchema(thisDs.schema, otherDs.schema, ignoreNullableFlag))
        throw new AssertionError(
          s"The data set schema is different :\n${SparkTest.displayErrSchema(thisDs.schema, otherDs.schema)}")
      else {
        if (checkRowOrder) {
          if (!thisDs.collect().sameElements(otherDs.collect())) {
            throw new AssertionError(s"The data set content is different :\n${displayErr(thisDs, otherDs)}")
          }
        } else {
          if (otherDs
            .except(thisDs)
            .head(1)
            .nonEmpty || thisDs.except(otherDs).head(1).nonEmpty) { // if sparkSQL => anti join ?
            throw new AssertionError(s"The data set content is different :\n${displayErr(thisDs, otherDs)}")
          }
        }
      }

    def assertEquals(seq: Seq[T]): Unit =
      assertEquals(seq.toDS(), ignoreSchemaFlag = true)

    def assertEquals(l: List[T]): Unit =
      assertEquals(l.toDS(), ignoreSchemaFlag = true)

    def assertApproxEquals(otherDs: Dataset[T], approx: Double, ignoreNullableFlag: Boolean = false): Unit = {
      val rows1 = dsToDf(thisDs).collect()
      val rows2 = dsToDf(otherDs).collect()
      val zipped = rows1.zip(rows2)
      if (SparkTest.compareSchema(thisDs.schema, otherDs.schema, ignoreNullableFlag))
        throw new AssertionError(
          s"The data set schema is different:\n${SparkTest.displayErrSchema(thisDs.schema, otherDs.schema)}")
      zipped.foreach {
        case (r1, r2) =>
          if (!areRowsEqual(r1, r2, approx))
            throw new AssertionError(s"$r1 was not equal approx to expected $r2, with a $approx approx")
      }
    }

    def dsToDf(ds: Dataset[T]): DataFrame = {
      val schema = ds.schema
      val df = ds.toDF()
      _sqlContext.createDataFrame(df.rdd, schema)
    }

    def displayErr(ds: Dataset[T], otherDs: Dataset[T]): String = {
      val actual = ds.collect().toSeq
      val expected = otherDs.collect().toSeq
      val errors = expected.zip(actual).filter(x => x._1 != x._2)

      errors.map(diff => s"${diff._1} was not equal to ${diff._2}").mkString("\n")
    }

  }

  // ========================== DATAFRAME ====================================

  implicit class SparkTestDfOps(thisDf: DataFrame) {
    thisDf.cache()

    /*
    def shouldForAll[T: Encoder](f: T => Boolean): Unit = {
      val count = thisDf.map(v => f(v)).filter(_ == true).count()
      if (thisDf.count() != count) { // !thisRdd.collect().forall(pred)
        val displayErr = thisDf.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }

    def shouldExists[T: Encoder](f: T => Boolean): Unit = {
      val size = thisDf.map(v => f(v)).filter(_ == true).count()
      if (size == 0) { // !thisRdd.collect().exists(pred)
        val displayErr = thisDf.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }
    }*/

    def assertEquals(otherDf: DataFrame,
                     checkRowOrder: Boolean = false,
                     ignoreNullableFlag: Boolean = false,
                     ignoreSchemaFlag: Boolean = false): Unit =
      if (!ignoreSchemaFlag && SparkTest.compareSchema(thisDf.schema, otherDf.schema, ignoreNullableFlag)) {
        throw new AssertionError(
          s"The data set schema is different\n${SparkTest.displayErrSchema(thisDf.schema, otherDf.schema)}")
      } else if (checkRowOrder) {
        if (!thisDf.collect().sameElements(otherDf.collect()))
          throw new AssertionError(s"The data set content is different :\n${displayErr(thisDf, otherDf)}")
      } else if (thisDf.except(otherDf).head(1).nonEmpty || otherDf.except(thisDf).head(1).nonEmpty) {
        throw new AssertionError(s"The data set content is different :\n${displayErr(thisDf, otherDf)}")
      }

    def assertEquals[T: Encoder](seq: Seq[T]): Unit =
      assertEquals(seq.toDF, ignoreSchemaFlag = true)

    def assertEquals[T: Encoder](l: List[T]): Unit =
      assertEquals(l.toDF, ignoreSchemaFlag = true)

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

    def displayErr(df: DataFrame, otherDf: DataFrame): String = {
      val actual = df.collect().toSeq
      val expected = otherDf.collect().toSeq
      val errors = expected.zip(actual).filter(x => x._1 != x._2)

      errors.map(diff => s"${diff._1} was not equal to ${diff._2}").mkString("\n")
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
      val rows1 = thisDf.collect()
      val rows2 = otherDf.collect()
      val zipped = rows1.zip(rows2)
      if (SparkTest.compareSchema(thisDf.schema, otherDf.schema, ignoreNullableFlag))
        throw new AssertionError(
          s"The data set schema is different\n${SparkTest.displayErrSchema(thisDf.schema, otherDf.schema)}")
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
      val expectedKeyed = thisRdd.map(x => (x, 1))
      val resultKeyed = otherRdd.map(x => (x, 1))

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
        val indexedResult = indexRDD(otherRdd)

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
                case (true, true) => (Some(thisIter.next()), Some(otherIter.next()))
                case _ => throw new Exception("next called when elements consumed")
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

  def compareSchema(sc1: StructType, sc2: StructType, ignoreNullableFlag: Boolean): Boolean =
    if (ignoreNullableFlag) {
      val newSc1 = setAllFieldsNullable(sc1)
      val newSc2 = setAllFieldsNullable(sc2)
      newSc1 != newSc2
    } else {
      sc1 != sc2
    }

  def displayErrSchema(actualSt: StructType, expectedSt: StructType): String = {
    val errors = expectedSt.zip(actualSt).filter(x => x._1 != x._2)

    errors.map(diff => s"${diff._1} was not equal to ${diff._2}").mkString("\n")
  }

  trait HasSparkSession {
    def ss: SparkSession
  }

  trait ReadOps extends HasSparkSession {

    def dfFromJsonString(json: String*): DataFrame = {
      val _ss = ss
      import _ss.implicits._

      ss.read.option("allowUnquotedFieldNames", value = true).json(ss.createDataset[String](json).rdd)
    }

    def dfFromJsonFile(path: String): DataFrame = ss.read.json(path)

  }

}
