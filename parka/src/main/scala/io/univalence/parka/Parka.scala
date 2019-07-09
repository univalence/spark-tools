package io.univalence.parka

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row }

case class DatasetInfo(source: Seq[String], nStage: Long)

case class Both[T](left: T, right: T) {
  def |>[U](f: (T, T) => U): U = f(left, right)
}

case class OuterInfo(
  nRow: Both[Long],
  byColumn: Seq[OuterInfo.ByColumn]
)

object OuterInfo {
  sealed trait ByColumn {
    def columnName: String
  }

  case class LongAggByColum(columnName: String, sum: Both[Long]) extends ByColumn
}

case class DeltaInfo(nRowEqual: Long,
                     nRowNotEqual: Long,
                     nbDiffByRow: Map[Int, Long],
                     byColumn: Seq[DeltaInfo.ByColumn])

object DeltaInfo {

  sealed trait ByColumn {
    def columnName: String
    def nEqual: Long
    def nNotEqual: Long
  }

  case class DeltaByColumnLong(columnName: String, nEqual: Long, nNotEqual: Long, sum: Both[Long], error: Long) extends ByColumn
}

case class DeltaQAResult(datasetInfo: Both[DatasetInfo], deltaInfo: DeltaInfo, outerInfo: OuterInfo)

object Parka { // ou DeltaQA comme vous voulez

  val keyValueSeparator = "§"

  def apply(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): DeltaQAResult = {
    //assert(key.nonEmpty)("you must have at least one key")
    //assert(left.schema == right.schema)("schemas are not equal : " + left.schema + " != " + right.schema)

    import org.apache.spark.sql.functions._
    import leftDs.sparkSession.implicits._

    val keyValue: Row => String = r => keyNames.map(r.getAs[Any]).mkString(keyValueSeparator)

    val leftAndRight: RDD[(String, (Iterable[Row], Iterable[Row]))] =
      leftDs.toDF.rdd.keyBy(keyValue).cogroup(rightDs.toDF.rdd.keyBy(keyValue))

    leftAndRight.map {
      case (key, (left, right)) =>
        // remove key from left and right (we dont compare key since they are equals)
        (left, right) match {
          case (l, r) if l.isEmpty => ???
          case (l, r) if r.isEmpty => ???
          case (l, r) => {

          }
        }
    }

    val contentNames = leftDs.columns.filter(!keyNames.contains(_))

    val datasetInfo = bothDatasetInfo(leftDs, rightDs)
    val deltaInfo   = ???
    val outerInfo   = newOuterInfo(leftDs, rightDs)(keyNames: _*)

    DeltaQAResult(datasetInfo, deltaInfo, outerInfo)
  }

  def datasetInfo(ds: Dataset[_]): DatasetInfo =
    DatasetInfo(ds.collect().map(_.toString), 0L)

  def bothDatasetInfo(leftDs: Dataset[_], rightDs: Dataset[_]): Both[DatasetInfo] =
    Both(datasetInfo(leftDs), datasetInfo(rightDs))

  def newOuterInfo(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): OuterInfo =
    OuterInfo(Both(leftDs.count, rightDs.count), ???)

  sealed trait Difference
  case class Alone(colname: String, )
  case class LongDifference(colname: String, difference: Long) extends Difference

  def compareRow(it: Iterable[(Row, Row)]): Seq[Difference] = ???
}
