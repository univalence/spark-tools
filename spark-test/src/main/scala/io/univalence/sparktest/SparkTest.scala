package io.univalence.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag
import io.univalence.sparktest.RowComparer._
import org.apache.spark.sql.types.StructType

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
      if (!_ds.collect().exists(pred)) {
        val displayErr = _ds.collect().take(10)
        throw new AssertionError(
          "No rows from the dataset match the predicate. " +
            s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}"
        )
      }

    def shouldForAll(pred: T => Boolean): Unit =
      if (!_ds.collect().forall(pred)) {
        val displayErr = _ds.collect().filterNot(pred).take(10)
        throw new AssertionError(
          "At least one row does not match the predicate. " +
            s"Rows not matching the predicate :\n${displayErr.mkString("\n")}"
        )
      }

    def assertContains(values: T*): Unit = {
      val dsArray = _ds.collect()
      if (!values.forall(dsArray.contains(_))) {
        val displayErr = values.diff(dsArray).take(10)
        throw new AssertionError(
          "At least one value was not in the dataset. " +
            s"Values not in the dataset :\n${displayErr.mkString("\n")}"
        )
      }
    }

    def assertEquals(ds: Dataset[T], checkRowOrder: Boolean = false, ignoreNullableFlag: Boolean = false): Unit =
      if (SparkTest.compareSchema(_ds.schema, ds.schema, ignoreNullableFlag))
        throw new AssertionError("The data set schema is different")
      else {
        if (checkRowOrder) {
          if (!_ds.collect().sameElements(ds.collect())) {
            throw new AssertionError(s"The data set content is different :\n${displayErr(_ds, ds)}")
          }
        } else {
          if (ds.except(_ds).head(1).nonEmpty || _ds.except(ds).head(1).nonEmpty) { // if sparkSQL => anti join ?
            throw new AssertionError(s"The data set content is different :\n${displayErr(_ds, ds)}")
          }
        }
      }

    def toDataframe(ds: Dataset[T]): DataFrame = {
      val schema = _ds.schema
      val df     = ds.toDF()
      _sqlContext.createDataFrame(df.rdd, schema)
    }

    def assertEquals(seq: Seq[T]): Unit = {
      val newDF = toDataframe(_ds)
      assertEquals(newDF.as[T])
    }

    def assertApproxEquals(ds: Dataset[T], approx: Double, ignoreNullableFlag: Boolean = false): Unit = {
      val rows1 = toDataframe(_ds).collect()
      val rows2 = toDataframe(ds).collect()
      val zipped = rows1.zip(rows2)
      if (SparkTest.compareSchema(_ds.schema, ds.schema, ignoreNullableFlag))
        throw new AssertionError("The data set schema is different")
      zipped.foreach { case (r1, r2) =>
        if (!areRowsEqual(r1, r2, approx))
          throw new AssertionError(s"$r1 was not equal approx to expected $r2, with a $approx approx")
      }
    }

    def displayErr(_ds: Dataset[T], ds: Dataset[T]): String = {
      val actual   = _ds.collect().toSeq
      val expected = ds.collect().toSeq
      val errors   = expected.zip(actual).filter(x => x._1 != x._2)

      errors.map(diff => s"${diff._1} was not equal to ${diff._2}").mkString("\n")
    }

  }

  implicit class SparkTestDfOps(df: DataFrame) {
    def assertEquals(otherDf: DataFrame, checkRowOrder: Boolean = false, ignoreNullableFlag: Boolean = false): Unit =
      if (SparkTest.compareSchema(df.schema, otherDf.schema, ignoreNullableFlag)) {
        throw new AssertionError("The data set schema is different")
      } else if (checkRowOrder) {
        if (!df.collect().sameElements(otherDf.collect()))
          throw new AssertionError(s"The data set content is different :\n${displayErr(df, otherDf)}")
      } else if (df.except(otherDf).head(1).nonEmpty || otherDf.except(df).head(1).nonEmpty) {
        throw new AssertionError(s"The data set content is different :\n${displayErr(df, otherDf)}")
      }

    def assertColumnEquality(rightLabel: String, leftLabel: String): Unit =
      if (compareColumn(rightLabel, leftLabel))
        throw new AssertionError("Columns are different")

    def compareColumn(rightLabel: String, leftLabel: String): Boolean = {
      val elements =
        df.select(
            rightLabel,
            leftLabel
          )
          .collect()

      elements.exists(r => r(0) != r(1))

      //val rightElements = elements.map(_(0))
      //val leftElements = elements.map(_(1))
      //rightElements.sameElements(leftElements)

      //val zippedElements = rightElements zip leftElements
      //zippedElements.filter{ case (right, left) => right != left }
    }

    def displayErr(df: DataFrame, otherDf: DataFrame): String = {
      val actual   = df.collect().toSeq
      val expected = otherDf.collect().toSeq
      val errors   = expected.zip(actual).filter(x => x._1 != x._2)

      errors.map(diff => s"${diff._1} was not equal to ${diff._2}").mkString("\n")
    }

    def assertApproxEquals(otherDf: DataFrame, approx: Double, ignoreNullableFlag: Boolean = false): Unit = {
      val rows1 = df.collect()
      val rows2 = otherDf.collect()
      val zipped = rows1.zip(rows2)
      if (SparkTest.compareSchema(df.schema, otherDf.schema, ignoreNullableFlag))
        throw new AssertionError("The data set schema is different")
      zipped.foreach { case (r1, r2) =>
        if (!areRowsEqual(r1, r2, approx))
          throw new AssertionError(s"$r1 was not equal approx to expected $r2, with a $approx approx")
      }
    }

    /**
      * Display the schema of the DataFrame as a case class.
      * Example :
      *   val df = Seq(1, 2, 3).toDF("id")
      *   df.showCaseClass("Id")
      *
      *   result :
      *   case class Id (
      *       id:Int
      *   )
      *
      * @param className name of the case class
      */
    def showCaseClass(className: String): Unit = {
      val s2cc = new Schema2CaseClass
      import s2cc.implicits._

      println(s2cc.schemaToCaseClass(df.schema, className))
    } //PrintCaseClass Definition from Dataframe inspection

  }

  implicit class SparkTestRDDOps[T: ClassTag](rdd: RDD[T]) {
    def assertRDDEquals(otherRDD: RDD[T]): Unit =
      if (compareRDD(otherRDD).isDefined)
        throw new AssertionError("The RDD is different")

    def compareRDD(otherRDD: RDD[T]): Option[(T, Int, Int)] = {
      val expectedKeyed = rdd.map(x      => (x, 1)).reduceByKey(_ + _)
      val resultKeyed   = otherRDD.map(x => (x, 1)).reduceByKey(_ + _)

      // Group them together and filter for difference
      expectedKeyed
        .cogroup(resultKeyed)
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
    }

    def assertRDDEqualsWithOrder(result: RDD[T]): Unit =
      if (compareRDDWithOrder(result).isDefined)
        throw new AssertionError("The RDD is different")

    def compareRDDWithOrder(result: RDD[T]): Option[(Option[T], Option[T])] =
      // If there is a known partitioner just zip
      if (result.partitioner.map(_ == rdd.partitioner.get).getOrElse(false)) {
        rdd.compareRDDWithOrderSamePartitioner(result)
      } else {
        // Otherwise index every element
        def indexRDD(rdd: RDD[T]): RDD[(Long, T)] =
          rdd.zipWithIndex.map { case (x, y) => (y, x) }
        val indexedExpected = indexRDD(rdd)
        val indexedResult   = indexRDD(result)

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
    def compareRDDWithOrderSamePartitioner(result: RDD[T]): Option[(Option[T], Option[T])] =
      // Handle mismatched lengths by converting into options and padding with Nones
      rdd
        .zipPartitions(result) { (thisIter, otherIter) =>
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

  def setAllFieldsNullable(sc: StructType): StructType = {
    StructType(sc.map(sf => sf.copy(nullable = true)))
  }

  def compareSchema(sc1: StructType, sc2: StructType, ignoreNullableFlag: Boolean): Boolean = {
    if (ignoreNullableFlag) {
      val newSc1 = setAllFieldsNullable(sc1)
      val newSc2 = setAllFieldsNullable(sc2)
      newSc1 != newSc2
    } else {
      sc1 != sc2
    }
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
