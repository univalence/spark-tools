package io.univalence.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

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
        throw new AssertionError("No rows from the dataset match the predicate. " +
          s"Rows not matching the predicate :\n ${displayErr.mkString("\n")}")
      }

    def shouldForAll(pred: T => Boolean): Unit =
      if (!_ds.collect().forall(pred)) {
        val displayErr = _ds.collect().filterNot(pred).take(10)
        throw new AssertionError("At least one row does not match the predicate. " +
          s"Rows not matching the predicate :\n${displayErr.mkString("\n")}")
      }

    def assertContains(values: T*): Unit = {
      val dsArray = _ds.collect()
      if (!values.forall(dsArray.contains(_))) {
        val displayErr = values.diff(dsArray).take(10)
        throw new AssertionError("At least one value was not in the dataset. " +
          s"Values not in the dataset :\n${displayErr.mkString("\n")}")
      }
    }

    def assertEquals(ds: Dataset[T]): Unit =
      if (_ds.schema != ds.schema)
        throw new AssertionError("The data set schema is different")
      else if (!_ds.collect().sameElements(ds.collect())) {
        val actual     = _ds.collect().toSeq
        val expected   = ds.collect().toSeq
        val errors     = expected.zip(actual).filter(x => x._1 != x._2)
        val displayErr = errors.map(diff => s"${diff._1} was not equal to ${diff._2}")
        throw new AssertionError("The data set content is different :\n" + displayErr.mkString("\n"))
      }

    def assertEquals(seq: Seq[T]): Unit = {
      val schema = _ds.schema
      val df     = seq.toDF()
      val newDF  = _sqlContext.createDataFrame(df.rdd, schema)
      assertEquals(newDF.as[T])
    }

  }

  implicit class SparkTestDfOps(df: DataFrame) {
    def assertEquals(otherDf: DataFrame, checkRowOrder: Boolean = false): Unit = {
      if (df.schema != otherDf.schema)
        throw new AssertionError("The data set schema is different")
      else {
        if (checkRowOrder) {
          if (!df.collect().sameElements(otherDf.collect()))
            throw new AssertionError("The data set content is different")
        } else {
          if (df.except(otherDf).head(1).nonEmpty || otherDf.except(df).head(1).nonEmpty) {
            throw new AssertionError("The data set content is different")
          }
        }
      }
    }

    def assertColumnEquality(rightLabel: String, leftLabel: String): Unit = {
      if(compareColumn(rightLabel, leftLabel))
        throw new AssertionError("Columns are different")

    }

    def compareColumn(rightLabel: String, leftLabel: String): Boolean = {
      val elements = df
        .select(
          rightLabel,
          leftLabel
        )
        .collect()
      val rightElements = elements.map(_(0))
      val leftElements = elements.map(_(1))
      elements.exists(r => r(0) != r(1))
      //rightElements.sameElements(leftElements)
      //val zippedElements = rightElements zip leftElements
      //zippedElements.filter{ case (right, left) => right != left }
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
      * @param className name of the case class
      */
    def showCaseClass(className: String): Unit = {
      val s2cc = new Schema2CaseClass
      import s2cc.implicits._
      println(s2cc.schemaToCaseClass(df.schema, className))
    } //PrintCaseClass Definition from Dataframe inspection

  }

  implicit class SparkTestRDDOps[T: ClassTag](rdd: RDD[T]) {
    def assertRDDEquals(otherRDD: RDD[T]): Unit = {
      if (compareRDD(otherRDD).isDefined)
        throw new AssertionError("The RDD is different")
    }

    def compareRDD(otherRDD: RDD[T]): Option[(T, Int, Int)] = {
      val expectedKeyed = rdd.map(x => (x, 1)).reduceByKey(_ + _)
      val resultKeyed = otherRDD.map(x => (x, 1)).reduceByKey(_ + _)
      // Group them together and filter for difference
      expectedKeyed.cogroup(resultKeyed).filter { case (_, (i1, i2)) =>
        i1.isEmpty || i2.isEmpty || i1.head != i2.head
      }
        .take(1).headOption.
        map { case (v, (i1, i2)) =>
          (v, i1.headOption.getOrElse(0), i2.headOption.getOrElse(0))
        }
    }

    def assertRDDEqualsWithOrder(result: RDD[T]): Unit = {
      if (compareRDDWithOrder(result).isDefined)
        throw new AssertionError("The RDD is different")

    }

    def compareRDDWithOrder(result: RDD[T]): Option[(Option[T], Option[T])] = {
      // If there is a known partitioner just zip
      if (result.partitioner.map(_ == rdd.partitioner.get).getOrElse(false)) {
        rdd.compareRDDWithOrderSamePartitioner(result)
      } else {
        // Otherwise index every element
        def indexRDD(rdd: RDD[T]): RDD[(Long, T)] = {
          rdd.zipWithIndex.map { case (x, y) => (y, x) }
        }
        val indexedExpected = indexRDD(rdd)
        val indexedResult = indexRDD(result)
        indexedExpected.cogroup(indexedResult).filter { case (_, (i1, i2)) =>
          i1.isEmpty || i2.isEmpty || i1.head != i2.head
        }.take(1).headOption.
          map { case (_, (i1, i2)) =>
            (i1.headOption, i2.headOption) }.take(1).headOption
      }
    }

    /**
      * Compare two RDDs. If they are equal returns None, otherwise
      * returns Some with the first mismatch. Assumes we have the same partitioner.
      */
    def compareRDDWithOrderSamePartitioner(result: RDD[T]): Option[(Option[T], Option[T])] = {
      // Handle mismatched lengths by converting into options and padding with Nones
      rdd.zipPartitions(result) {
        (thisIter, otherIter) =>
          new Iterator[(Option[T], Option[T])] {
            def hasNext: Boolean = (thisIter.hasNext || otherIter.hasNext)

            def next(): (Option[T], Option[T]) = {
              (thisIter.hasNext, otherIter.hasNext) match {
                case (false, true) => (Option.empty[T], Some(otherIter.next()))
                case (true, false) => (Some(thisIter.next()), Option.empty[T])
                case (true, true) => (Some(thisIter.next()), Some(otherIter.next()))
                case _ => throw new Exception("next called when elements consumed")
              }
            }
          }
      }.filter { case (v1, v2) => v1 != v2 }.take(1).headOption
    }
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

      ss.read.option("allowUnquotedFieldNames", value = true).json(ss.createDataset[String](json).rdd)
    }
    def dfFromJsonFile(path: String): DataFrame = ss.read.json(path)
  }
}
