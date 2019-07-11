package io.univalence.parka

import io.univalence.parka
import io.univalence.parka.Delta.DeltaLong
import io.univalence.parka.Describe.DescribeLong
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row }
import io.univalence.sparktest.ValueComparison._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

case class DatasetInfo(source: Seq[String], nStage: Long)

case class Both[+T](left: T, right: T) {
  def fold[U](f: (T, T) => U): U = f(left, right)

  def map[U](f: T => U): Both[U] = Both(f(left), f(right))
}

case class Outer(countRow: Both[Long], byColumn: Map[String, Both[Describe]])

sealed trait Describe extends Serializable

object Describe {
  case class DescribeLong(sum: Long) extends Describe

  implicit val describeMono: Monoid[Describe] = Monoid.gen[DescribeLong].asInstanceOf[Monoid[Describe]]
}

case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDiffByRow: Map[Int, Long],
                 byColumn: Map[String, Delta])

//case class ByColumn[T](columnName:String, value:T)

trait Monoid[T] extends Serializable {
  def combine(x: T, y: T): T
  def empty: T
}

object Monoid {

  def empty[T: Monoid]: T = implicitly[Monoid[T]].empty

  def apply[T](_empty: T, _combine: (T, T) => T): Monoid[T] = new Monoid[T] {
    override def combine(x: T, y: T): T = _combine(x, y)
    override def empty: T               = _empty
  }

  implicit def mapMonoid[K, V: Monoid]: Monoid[Map[K, V]] =
    Monoid(
      Map.empty,
      (m1, m2) => {
        (m1.keySet ++ m2.keySet)
          .map(
            k =>
              k -> (m1.get(k) match {
                case None => m2.getOrElse(k, implicitly[Monoid[V]].empty)
                case Some(x) =>
                  m2.get(k) match {
                    case Some(y) => implicitly[Monoid[V]].combine(x, y)
                    case None    => x
                  }
              })
          )
          .toMap
      }
    )

  implicit object longMonoid extends Monoid[Long] {
    @inline
    final override def combine(x: Long, y: Long): Long = x + y
    @inline
    final override def empty: Long = 0L
  }

  implicit class MonoidOps[T: Monoid](t: T) {
    def +(t2: T): T = implicitly[Monoid[T]].combine(t, t2)
  }

  type Typeclass[T] = Monoid[T]

  import magnolia._

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] =
    new Typeclass[T] {
      override def combine(x: T, y: T): T =
        if (x == empty) y
        else if (y == empty) x
        else caseClass.construct(param => param.typeclass.combine(param.dereference(x), param.dereference(y)))
      override lazy val empty: T = caseClass.construct(param => param.typeclass.empty)
    }

  //def dispatch[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = ???

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}

sealed trait Delta extends Serializable {
  def nEqual: Long
  def nNotEqual: Long
  def describe: Both[Describe]
}

object Delta {
  case class DeltaLong(nEqual: Long, nNotEqual: Long, describe: Both[DescribeLong], error: Long) extends Delta

  implicit val deltaMonoid: Monoid[Delta] = Monoid.gen[DeltaLong].asInstanceOf[Monoid[Delta]]

}

case class ParkaResult(inner: Inner, outer: Outer)

case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result: ParkaResult)

object Parka {

  private val keyValueSeparator = "§"

  def describe(row: Row)(keys: Set[String]): Map[String, Describe] = {
    val fields = row.asInstanceOf[GenericRowWithSchema].schema.fieldNames

    fields
      .filterNot(keys)
      .map(name => {
        val describe = row.getAs[Any](name) match {
          case l: Long => DescribeLong(l)
        }

        name -> describe
      })
      .toMap
  }

  def outer(row: Row, side: Side)(keys: Set[String]): Outer =
    Outer(
      countRow = side match {
        case Right => Both(left = 0, right = 1)
        case Left  => Both(left = 1, right = 0)
      },
      byColumn = describe(row)(keys)
        .mapValues(
          d =>
            side match {
              case Right => Both(left = emptyDescribe, right = d)
              case Left  => Both(left = d, right             = emptyDescribe)
          }
        )
        .map(identity) // oH No https://www.youtube.com/watch?v=P-3GOo_nWoc
    )

  def inner(left: Row, right: Row)(keys: Set[String]): Inner = {

    val schema = left.asInstanceOf[GenericRowWithSchema].schema

    val byNames: Map[String, Delta] = schema.fieldNames
      .filterNot(keys)
      .map(name => {
        val delta: Delta = (left.getAs[Any](name), right.getAs[Any](name)) match {
          case (x: Long, y: Long) =>
            if (x == y) {
              val describe = DescribeLong(x)
              DeltaLong(1, 0, Both(describe, describe), 0)
            } else {
              val diff = x - y
              DeltaLong(0, 1, Both(DescribeLong(x), DescribeLong(y)), diff * diff)
            }
        }
        name -> delta
      })
      .toMap

    val isEqual = byNames.forall(_._2.nEqual == 1)
    val nDiff   = if (isEqual) 0 else byNames.count(_._2.nNotEqual == 1)
    Inner(if (isEqual) 1 else 0, if (isEqual) 0 else 1, Map(nDiff -> 1), byNames)
  }

  private val emptyInner: Inner        = Monoid.empty
  private val emptyOuter: Outer        = Monoid.empty
  private val emptyDescribe: Describe  = Monoid.empty
  private val emptyResult: ParkaResult = Monoid.empty

  def result(left: Iterable[Row], right: Iterable[Row])(keys: Set[String]): ParkaResult =
    (left, right) match {
      //Only  Right
      case (l, r) if l.isEmpty && r.nonEmpty => ParkaResult(emptyInner, outer(r.head, Right)(keys))
      //Only Left
      case (l, r) if l.nonEmpty && r.isEmpty => ParkaResult(emptyInner, outer(l.head, Left)(keys))
      //Inner
      case (l, r) if l.nonEmpty && r.nonEmpty => ParkaResult(inner(l.head, r.head)(keys), emptyOuter)
    }

  private val monoidParkaResult = Monoid.gen[ParkaResult]
  def combine(left: ParkaResult, right: ParkaResult): ParkaResult =
    monoidParkaResult.combine(left, right)

  def apply(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis = {
    assert(keyNames.nonEmpty, "you must have at least one key")
    assert(leftDs.schema == rightDs.schema, "schemas are not equal : " + leftDs.schema + " != " + rightDs.schema)

    import org.apache.spark.sql.functions._
    import leftDs.sparkSession.implicits._

    val schema = leftDs.schema

    val keyValue: Row => String = r => keyNames.map(r.getAs[Any]).mkString(keyValueSeparator)

    val keys = keyNames.toSet

    val leftAndRight: RDD[(String, (Iterable[Row], Iterable[Row]))] =
      leftDs.toDF.rdd.keyBy(keyValue).cogroup(rightDs.toDF.rdd.keyBy(keyValue))

    val res = leftAndRight
      .map({
        case (k, (left, right)) => result(left, right)(keys)
      })
      .reduce(combine)

    ParkaAnalysis(datasetInfo = Both(leftDs, rightDs).map(datasetInfo), result = res)
  }

  def datasetInfo(ds: Dataset[_]): DatasetInfo = DatasetInfo(Nil, 0L)

  sealed trait Side
  case object Left extends Side
  case object Right extends Side
}
