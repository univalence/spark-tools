package io.univalence.parka

import io.univalence.parka.Describe.DescribeLong
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import io.univalence.sparktest.ValueComparison._

case class DatasetInfo(source: Seq[String], nStage: Long)

case class Both[+T](left: T, right: T) {
  def |>[U](f: (T, T) => U): U = f(left, right)
}

case class Outer(countRow: Both[Long], byColumn: Map[String,Both[Describe]])


sealed trait Describe

object Describe {
  case class DescribeLong(sum:Long) extends Describe


  implicit val describeMono:Monoid[Describe] = Monoid.gen[DescribeLong].asInstanceOf[Monoid[Describe]]
}


case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDiffByRow: Map[Int, Long],
                 byColumn: Map[String,Delta])


//case class ByColumn[T](columnName:String, value:T)


trait Monoid[T] {
  def combine(x:T,y:T):T
  def empty:T
}

object Monoid {

  def empty[T:Monoid]:T = implicitly[Monoid[T]].empty

  def apply[T](_empty:T, _combine: (T,T) => T): Monoid[T] = new Monoid[T] {
    override def combine(x: T, y: T): T = _combine(x,y)
    override def empty: T = _empty
  }

  implicit def mapMonoid[K,V:Monoid]:Monoid[Map[K,V]] = Monoid(Map.empty,(m1,m2) => {
    (m1.keySet ++ m2.keySet).map(k => k -> (m1.get(k) match {
      case None => m2.getOrElse(k, implicitly[Monoid[V]].empty)
      case Some(x) => m2.get(k) match {
        case Some(y) => implicitly[Monoid[V]].combine(x,y)
        case None => x
      }
    })).toMap
  })

  implicit object longMonoid extends Monoid[Long] {
    @inline
    final override def combine(x: Long, y: Long): Long = x + y
    @inline
    final override def empty: Long = 0L
  }

  implicit class MonoidOps[T:Monoid](t:T) {
    def +(t2:T):T = implicitly[Monoid[T]].combine(t,t2)
  }

  type Typeclass[T] = Monoid[T]

  import magnolia._

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = {
    new Typeclass[T] {
      override def combine(x: T, y: T): T = {
        caseClass.construct(param =>
          param.typeclass.combine(param.dereference(x), param.dereference(y)))
      }

      override def empty: T = caseClass.construct(param => param.typeclass.empty)
    }
  }

  //def dispatch[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = ???

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}


sealed trait Delta {
  def nEqual: Long
  def nNotEqual: Long
  def describe:Both[Describe]
}

object Delta {
  case class DeltaLong(nEqual: Long,
                       nNotEqual: Long,
                       describe: Both[DescribeLong],
                       error: Long) extends Delta




  implicit val deltaMonoid:Monoid[Delta] = Monoid.gen[DeltaLong].asInstanceOf[Monoid[Delta]]

}









case class ParkaResult(inner: Inner, outer: Outer)

case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result:ParkaResult)

object Parka {



  val parkaResultM = Monoid.gen[ParkaResult]


  private val keyValueSeparator = "§"

  def apply(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis = {
    assert(keyNames.nonEmpty, "you must have at least one key")
    assert(leftDs.schema == rightDs.schema, "schemas are not equal : " + leftDs.schema + " != " + rightDs.schema)

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

            val differences = compareRow(lRow, rRow)

            Crowd(key, differences, Both(lRow, rRow))
        }
    }




    val (countRow, countEq, countNotEq, allDifferences) =
      diffs.aggregate((Both(0L, 0L), 0L, 0L, Seq.empty[Seq[Difference]]))(
        {
          case ((Both(l: Long, r: Long), countEq: Long, countNotEq: Long, allDifferences: Seq[Difference]),
          diff) =>
            diff match {
              case Alone(_, Right, _) => (Both(l, r + 1), countEq, countNotEq, allDifferences)
              case Alone(_, Left, _)  => (Both(l + 1, r), countEq, countNotEq, allDifferences)
              case Crowd(_, differences: Seq[Difference], _) if differences != Seq.empty =>
                (Both(l, r), countEq, countNotEq + 1, differences +: allDifferences)
              case Crowd(_, _, _) => (Both(l, r), countEq + 1, countNotEq, allDifferences)
            }
        }, {
          case ((Both(ll, lr), lcountEq, lcountNotEq, lallDifferences),
          (Both(rl, rr), rcountEq, rcountNotEq, rallDifferences)) =>
            (Both(ll + rl, lr + rr), lcountEq + rcountEq, lcountNotEq + rcountNotEq, lallDifferences ++ rallDifferences)
        }
      )

    val differences = allDifferences.flatten.map{
      case LongDifference(key, value) => (key, value)
      case _ => ("error", 0)
    }.groupBy(_._1).map { case (k, v) => k -> v.map { _._2}} //groupbykey made in scala

    val contentNames = leftDs.columns.filter(!keyNames.contains(_)).toList

    println(differences)

    val datasetInfo = bothDatasetInfo(leftDs, rightDs)
    val deltaInfo   = Inner(countEq, countNotEq, Map.empty, Map.empty)
    val outerInfo   = Outer(countRow, Map.empty)

    ParkaAnalysis(datasetInfo, ParkaResult(deltaInfo, outerInfo))
  }

  def datasetInfo(ds: Dataset[_]): DatasetInfo =
    DatasetInfo(ds.collect().map(_.toString), 0L)

  def bothDatasetInfo(leftDs: Dataset[_], rightDs: Dataset[_]): Both[DatasetInfo] =
    Both(datasetInfo(leftDs), datasetInfo(rightDs))

  def newOuterInfo(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): Outer =
    Outer(Both(leftDs.count, rightDs.count), ???)

  sealed trait Side
  case object Left extends Side
  case object Right extends Side

  sealed trait RowComparison
  case class Alone(key: String, side: Side, row: Row) extends RowComparison
  case class Crowd(key: String, differences: Seq[Difference], rows: Both[Row]) extends RowComparison

  sealed trait Difference
  case class LongDifference(colname: String, difference: Long) extends Difference
  case class UnknowDifference(om: ObjectModification) extends Difference

  def compareRow(left: Row, right: Row): Seq[Difference] = { //left != right
    val differences = compareValue(fromRow(left), fromRow(right))
    differences.map(exploitObjectModification)
  }

  def exploitObjectModification(om: ObjectModification): Difference = om match {
    case ObjectModification(path, AddValue(AtomicValue(v))) =>
      LongDifference(path.firstName, v.asInstanceOf[Number].longValue)
    case ObjectModification(path, RemoveValue(AtomicValue(v))) =>
      LongDifference(path.firstName, -1 * v.asInstanceOf[Number].longValue)
    case ObjectModification(path, ChangeValue(AtomicValue(lv), AtomicValue(rv))) =>
      LongDifference(path.firstName, rv.asInstanceOf[Number].longValue - lv.asInstanceOf[Number].longValue)
    case _ => UnknowDifference(om)
  }

  //
}
