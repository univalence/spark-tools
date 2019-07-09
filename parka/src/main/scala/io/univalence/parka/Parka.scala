package io.univalence.parka

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row }
import io.univalence.sparktest.ValueComparison._

case class DatasetInfo(source: Seq[String], nStage: Long)

case class Both[T](left: T, right: T) {
  def |>[U](f: (T, T) => U): U = f(left, right)
}

case class OuterInfo(
  countRow: Both[Long],
  byColumn: Seq[OuterInfo.ByColumn]
)

object OuterInfo {
  sealed trait ByColumn {
    def columnName: String
  }

  case class LongAggByColumn(columnName: String, sum: Both[Long]) extends ByColumn
}

case class DeltaInfo(countRowEqual: Long,
                     countRowNotEqual: Long,
                     countDiffByRow: Map[Int, Long],
                     byColumn: Seq[DeltaInfo.ByColumn])

object DeltaInfo {

  sealed trait ByColumn {
    def columnName: String
    def nEqual: Long
    def nNotEqual: Long
  }

  case class DeltaByColumnLong(columnName: String, nEqual: Long, nNotEqual: Long, sum: Both[Long], error: Long)
      extends ByColumn
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

    val diffs: RDD[RowComparison] = leftAndRight.map {
      case (key, (left: Iterable[Row], right: Iterable[Row])) =>
        // remove key from left and right (we dont compare key since they are equals)
        (left, right) match {
          case (l, r) if l.isEmpty => Alone(key, Right, r.head)
          case (l, r) if r.isEmpty => Alone(key, Left, l.head)
          case (l, r) =>
            val lRow: Row = l.head
            val rRow: Row = r.head
            Crowd(key, compareRow(lRow, rRow))
        }
    }

    val (countRow, countEq, countNotEq) =
      diffs.aggregate((Both(0L, 0L), 0L, 0L))(
        {
          case ((Both(l: Long, r: Long), countEq: Long, countNotEq: Long), diff) =>
            diff match {
              case Alone(_, Right, _) => (Both(l, r + 1), countEq, countNotEq)
              case Alone(_, Left, _)  => (Both(l + 1, r), countEq, countNotEq)
              case Crowd(_, true)     => (Both(l, r), countEq, countNotEq + 1)
              case Crowd(_, _)        => (Both(l, r), countEq + 1, countNotEq)
            }
        }, {
          case ((Both(ll, lr), lcountEq, lcountNotEq), (Both(rl, rr), rcountEq, rcountNotEq)) =>
            (Both(ll + rl, lr + rr), lcountEq + rcountEq, lcountNotEq + rcountNotEq)
        }
      )

    val contentNames = leftDs.columns.filter(!keyNames.contains(_))

    val datasetInfo = bothDatasetInfo(leftDs, rightDs)
    val deltaInfo   = DeltaInfo(countEq, countNotEq, Map.empty, Seq.empty)
    val outerInfo   = OuterInfo(countRow, Seq.empty)

    DeltaQAResult(datasetInfo, deltaInfo, outerInfo)
  }

  def datasetInfo(ds: Dataset[_]): DatasetInfo =
    DatasetInfo(ds.collect().map(_.toString), 0L)

  def bothDatasetInfo(leftDs: Dataset[_], rightDs: Dataset[_]): Both[DatasetInfo] =
    Both(datasetInfo(leftDs), datasetInfo(rightDs))

  def newOuterInfo(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): OuterInfo =
    OuterInfo(Both(leftDs.count, rightDs.count), ???)

  sealed trait Side
  case object Left extends Side
  case object Right extends Side

  sealed trait RowComparison
  case class Alone(key: String, side: Side, row: Row) extends RowComparison
  case class Crowd(key: String, different: Boolean) extends RowComparison

  //sealed trait Difference
  //case class LongDifference(colname: String, difference: Long) extends Difference

  def compareRow(left: Row, right: Row): Boolean = left != right
  //compareValue(fromRow(left), fromRow(right)).nonEmpty

}
