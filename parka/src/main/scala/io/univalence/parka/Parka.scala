package io.univalence.parka

import java.sql.{ Date, Timestamp }

import cats.kernel.Monoid
/*import io.univalence.parka.Describe.{
  DescribeBoolean,
  DescribeDate,
  DescribeDouble,
  DescribeLong,
  DescribeString,
  DescribeTimestamp
}*/
import io.univalence.parka.MonoidGen._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{ Dataset, Row }

case class Both[+T](left: T, right: T) {
  def fold[U](f: (T, T) => U): U = f(left, right)

  def map[U](f: T => U): Both[U] = Both(f(left), f(right))
}

case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result: ParkaResult)

case class DatasetInfo(source: Seq[String], nStage: Long)
case class ParkaResult(inner: Inner, outer: Outer)

/**
  * Inner contains information about rows with similar keys
  *
  * @param countRowEqual          The number of equal rows (with the same values for each columns which are not keys)
  * @param countRowNotEqual       The number of unequal rows
  * @param countDiffByRow         Map with number of differences between two rows as a key and for each them how many times they occured between the two Datasets
  * @param byColumn               Map with column name as a key and for each of them two Describe, one for each row's Dataset - only for inner row
  */
case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDiffByRow: Map[Seq[String], Long],
                 byColumn: Map[String, DeltaV2])

/**
  * Outer contains information about rows that are only present in one of the two datasets
  *
  * @param countRow               The number of additional rows for each Datasets
  * @param byColumn               Map with column name as a key and for each of them two Describe, one for each row's Dataset - only for outer row
  */
case class Outer(countRow: Both[Long], byColumn: Map[String, Both[Describe]])

/*sealed trait Describe extends Serializable {
  def nnull: Long
}*/

trait CoProductMonoidHelper[T] {
  type Combined <: T
  def lift(t: T): Combined
  def unlift(t: Combined): T
}

object CoProductMonoidHelper {
  type Aux[In, Out <: In] = CoProductMonoidHelper[In] {
    type Combined = Out
  }
}

/*object Describe {

  val empty = DescribeCombine(None, None, None, None, None, None, 0)

  implicit val coProductMonoidHelper: CoProductMonoidHelper.Aux[Describe, DescribeCombine] =
    new CoProductMonoidHelper[Describe] {
      override type Combined = DescribeCombine

      override def lift(t: Describe): DescribeCombine = t match {
        case dc: DescribeCombine   => dc
        case dl: DescribeLong      => empty.copy(long = Some(dl))
        case dd: DescribeDouble    => empty.copy(double = Some(dd))
        case ds: DescribeString    => empty.copy(string = Some(ds))
        case db: DescribeBoolean   => empty.copy(boolean = Some(db))
        case dd: DescribeDate      => empty.copy(date = Some(dd))
        case dt: DescribeTimestamp => empty.copy(timestamp = Some(dt))
      }

      def unlift(combined: Combined): Describe =
        combined match {
          case DescribeCombine(Some(x), None, None, None, None, None, nnull) => x.copy(nnull = x.nnull + nnull)
          case DescribeCombine(None, Some(x), None, None, None, None, nnull) => x.copy(nnull = x.nnull + nnull)
          case DescribeCombine(None, None, Some(x), None, None, None, nnull) => x.copy(nnull = x.nnull + nnull)
          case DescribeCombine(None, None, None, Some(x), None, None, nnull) => x.copy(nnull = x.nnull + nnull)
          case DescribeCombine(None, None, None, None, Some(x), None, nnull) => x.copy(nnull = x.nnull + nnull)
          case DescribeCombine(None, None, None, None, None, Some(x), nnull) => x.copy(nnull = x.nnull + nnull)
          case _                                                             => combined
        }
    }

  case class DescribeLong(value: Histogram, nnull: Long) extends Describe
  case class DescribeDouble(value: Histogram, nnull: Long) extends Describe
  case class DescribeString(length: Histogram, nnull: Long) extends Describe
  case class DescribeBoolean(nTrue: Long, nFalse: Long, nnull: Long) extends Describe
  case class DescribeDate(period: Histogram, nnull: Long) extends Describe
  case class DescribeTimestamp(period: Histogram, nnull: Long) extends Describe

  case class DescribeCombine(long: Option[DescribeLong],
                             double: Option[DescribeDouble],
                             string: Option[DescribeString],
                             boolean: Option[DescribeBoolean],
                             date: Option[DescribeDate],
                             timestamp: Option[DescribeTimestamp],
                             nnull: Long)
      extends Describe

  def apply(n: Null): DescribeCombine                = DescribeCombine(None, None, None, None, None, None, 1)
  def apply(long: Long): DescribeLong                = DescribeLong(Histogram.value(long), 0)
  def apply(long: Double): DescribeDouble            = DescribeDouble(Histogram.value(long), 0)
  def apply(string: String): DescribeString          = DescribeString(Histogram.value(string.length.toLong), 0)
  def apply(boolean: Boolean): DescribeBoolean       = if (boolean) DescribeBoolean(1, 0, 0) else DescribeBoolean(0, 1, 0)
  def apply(date: Date): DescribeDate                = DescribeDate(Histogram.value(date.getTime), 0)
  def apply(timestamp: Timestamp): DescribeTimestamp = DescribeTimestamp(Histogram.value(timestamp.getTime), 0)

  def apply(any: Any): Describe = any match {
    case null         => Describe(null)
    case l: Long      => Describe(l)
    case d: Double    => Describe(d)
    case s: String    => Describe(s)
    case b: Boolean   => Describe(b)
    case d: Date      => Describe(d)
    case t: Timestamp => Describe(t)
  }
}*/

case class Describe(count: Long, histograms: Map[String, Histogram], counts: Map[String, Long])

object Describe {

  val oneValue: Describe = Describe(1, Map.empty, Map.empty)

  final private def histo(name: String, l: Long) =
    oneValue.copy(histograms = Map(name -> Histogram.value(l)))
  final private def histo(name: String, d: Double) =
    oneValue.copy(histograms = Map(name -> Histogram.value(d)))
  final private def count(name: String, value: Long) = oneValue.copy(counts = Map(name -> value))

  def apply(a: Any): Describe =
    a match {
      case null          => count("nNull", 1)
      case true          => count("nTrue", 1)
      case false         => count("nFalse", 1)
      case s: String     => histo("length", s.length)
      case d: Double     => histo("value", d)
      case l: Long       => histo("value", l)
      case ts: Timestamp => histo("timestamp", ts.getTime)
      case d: Date       => histo("date", d.getTime)
      case (b1: Boolean, b2: Boolean) =>
        val key = (if(b1) "t" else "f") + (if(b2) "t" else "f")
        count(key, 1)
    }
}

case class DeltaV2(nEqual: Long, nNotEqual: Long, describe: Both[Describe], error: Describe)

object DeltaV2 {
  val emptyDescribe: Describe = MonoidUtils.describeMonoid.empty
  def levenshtein(s1: String, s2: String): Int = {
    import scala.math._
    def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }
    for {
      j <- 1 to s2.length
      i <- 1 to s1.length
    } dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else
        minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
    dist(s2.length)(s1.length)
  }

  def stringDiff(str1: String, str2: String): Long =
  //Pour l'instant on va faire ça
    levenshtein(str1, str2).toLong

  def error(x: Any, y: Any): Describe = {
    (x, y) match {
      case (null, _) => Describe(null)
      case (_, null) => Describe(null)
      case (l1: Long, l2: Long) => Describe(l1 - l2)
      case (d1: Double, d2: Double) => Describe(d1 - d2)
      case (s1: String, s2: String) => Describe(stringDiff(s1, s2))
      case (b1: Boolean, b2: Boolean) => Describe((b1, b2))
      case (d1: Date, d2: Date) => Describe(d1.getTime - d2.getTime)
      case (t1: Timestamp, t2: Timestamp) => Describe(t1.getTime - t2.getTime)
    }
  }

  def apply(x: Any, y: Any): DeltaV2 = {
    if (x == y) DeltaV2(1, 0, Both(Describe(x), Describe(y)), emptyDescribe)
    else DeltaV2(0, 1, Both(Describe(x), Describe(y)), error(x, y))
  }
}

/*sealed trait Delta extends Serializable {
  def nEqual: Long
  def nNotEqual: Long
  def describe: Both[Describe]
  def toNull: Both[Long]
}

object Delta {

  def apply(l1: Long, l2: Long): DeltaLong =
    if (l1 == l2) {
      val describe = Describe(l1)
      DeltaLong(1, 0, Both(describe, describe), Histogram.value(0), Both(0, 0))
    } else {
      val diff = l1 - l2
      DeltaLong(0, 1, Both(Describe(l1), Describe(l2)), Histogram.value(diff), Both(0, 0))
    }
  def apply(s1: Null, s2: Long): DeltaLong =
    DeltaLong(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(1, 0))
  def apply(s1: Long, s2: Null): DeltaLong =
    DeltaLong(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(0, 1))

  def apply(d1: Double, d2: Double): DeltaDouble =
    if (d1 == d2) {
      val describe = Describe(d1)
      DeltaDouble(1, 0, Both(describe, describe), Histogram.value(0), Both(0, 0))
    } else {
      val diff = d1 - d2
      DeltaDouble(0, 1, Both(Describe(d1), Describe(d2)), Histogram.value(diff), Both(0, 0))
    }
  def apply(s1: Null, s2: Double): DeltaDouble =
    DeltaDouble(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(1, 0))
  def apply(s1: Double, s2: Null): DeltaDouble =
    DeltaDouble(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(0, 1))

  def apply(s1: String, s2: String): DeltaString =
    if (s1 == s2) {
      val describe = Describe(s1)
      DeltaString(1, 0, Both(describe, describe), Histogram.value(0), Both(0, 0))
    } else {
      val diff = Delta.stringDiff(s1, s2)
      DeltaString(0, 1, Both(Describe(s1), Describe(s2)), Histogram.value(diff), Both(0, 0))
    }
  def apply(s1: Null, s2: String): DeltaString =
    DeltaString(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(1, 0))
  def apply(s1: String, s2: Null): DeltaString =
    DeltaString(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(0, 1))

  def apply(b1: Boolean, b2: Boolean): DeltaBoolean = {
    val d1 = Describe(b1)
    if (b1 == b2)
      DeltaBoolean(1, 0, Both(d1, d1), Both(0, 0))
    else
      DeltaBoolean(0, 1, Both(d1, Describe(b2)), Both(0, 0))
  }
  def apply(s1: Null, s2: Boolean): DeltaBoolean =
    DeltaBoolean(0, 1, Both(Describe(null), Describe(s2)), Both(1, 0))
  def apply(s1: Boolean, s2: Null): DeltaBoolean =
    DeltaBoolean(0, 1, Both(Describe(null), Describe(s2)), Both(0, 1))

  def levenshtein(s1: String, s2: String): Int = {
    import scala.math._
    def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }
    for {
      j <- 1 to s2.length
      i <- 1 to s1.length
    } dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else
        minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
    dist(s2.length)(s1.length)
  }

  def stringDiff(str1: String, str2: String): Long =
    //Pour l'instant on va faire ça
    levenshtein(str1, str2).toLong

  def apply(x: Date, y: Date): DeltaDate = {
    val d1     = Describe(x)
    val period = math.abs(x.getTime - y.getTime)
    if (period == 0)
      DeltaDate(1, 0, Both(d1, d1), Histogram.value(period), Both(0, 0))
    else
      DeltaDate(0, 1, Both(d1, Describe(y)), Histogram.value(period), Both(0, 0))
  }
  def apply(s1: Null, s2: Date): DeltaDate =
    DeltaDate(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(1, 0))
  def apply(s1: Date, s2: Null): DeltaDate =
    DeltaDate(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(0, 1))

  def apply(x: Timestamp, y: Timestamp): DeltaTimestamp = {
    val d1     = Describe(x)
    val period = math.abs(x.getTime - y.getTime)
    if (period == 0)
      DeltaTimestamp(1, 0, Both(d1, d1), Histogram.value(period), Both(0, 0))
    else
      DeltaTimestamp(0, 1, Both(d1, Describe(y)), Histogram.value(period), Both(0, 0))
  }
  def apply(s1: Null, s2: Timestamp): DeltaTimestamp =
    DeltaTimestamp(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(1, 0))
  def apply(s1: Timestamp, s2: Null): DeltaTimestamp =
    DeltaTimestamp(0, 1, Both(Describe(null), Describe(s2)), Histogram.value(0), Both(0, 1))

  case class DeltaLong(nEqual: Long, nNotEqual: Long, describe: Both[Describe], error: Histogram, toNull: Both[Long])
      extends Delta

  case class DeltaDouble(nEqual: Long,
                         nNotEqual: Long,
                         describe: Both[Describe],
                         error: Histogram,
                         toNull: Both[Long])
      extends Delta

  case class DeltaString(nEqual: Long,
                         nNotEqual: Long,
                         describe: Both[Describe],
                         error: Histogram,
                         toNull: Both[Long])
      extends Delta

  case class DeltaBoolean(nEqual: Long, nNotEqual: Long, describe: Both[Describe], toNull: Both[Long]) extends Delta {
    val nTrueLeft: Long  = describe.left.counts.getOrElse("nTrue", 0)
    val nTrueRight: Long = describe.right.counts.getOrElse("nTrue", 0)
    def tt: Long         = (nTrueLeft + nTrueRight - nNotEqual) / 2
    def tf: Long         = (nNotEqual + nTrueLeft - nTrueRight) / 2
    def ff: Long         = nEqual - tt
    def ft: Long         = nNotEqual - tf
  }

  case class DeltaDate(nEqual: Long, nNotEqual: Long, describe: Both[Describe], error: Histogram, toNull: Both[Long])
      extends Delta
  case class DeltaTimestamp(nEqual: Long,
                            nNotEqual: Long,
                            describe: Both[Describe],
                            error: Histogram,
                            toNull: Both[Long])
      extends Delta

  case class DeltaCombine(long: Option[DeltaLong],
                          double: Option[DeltaDouble],
                          string: Option[DeltaString],
                          boolean: Option[DeltaBoolean],
                          date: Option[DeltaDate],
                          timestamp: Option[DeltaTimestamp],
                          toNull: Both[Long])
      extends Delta {

    @transient lazy val seq: Seq[Delta]          = Seq(long, double, string, boolean).flatten
    @transient override lazy val nEqual: Long    = seq.map(_.nEqual).sum
    @transient override lazy val nNotEqual: Long = seq.map(_.nNotEqual).sum
    @transient override lazy val describe: Both[Describe] = {
      val monoid = MonoidUtils.bothDescribeMonoid
      seq.map(_.describe).reduceOption(monoid.combine).getOrElse(monoid.empty)
    }
  }

  val empty: DeltaCombine = DeltaCombine(None, None, None, None, None, None, Both(0, 0))

  implicit val coProductMonoidHelper: CoProductMonoidHelper.Aux[Delta, DeltaCombine] =
    new CoProductMonoidHelper[Delta] {
      override type Combined = DeltaCombine

      override def lift(t: Delta): DeltaCombine =
        t match {
          case dc: DeltaCombine   => dc
          case dl: DeltaLong      => empty.copy(long = Some(dl))
          case dl: DeltaDouble    => empty.copy(double = Some(dl))
          case ds: DeltaString    => empty.copy(string = Some(ds))
          case db: DeltaBoolean   => empty.copy(boolean = Some(db))
          case dd: DeltaDate      => empty.copy(date = Some(dd))
          case dt: DeltaTimestamp => empty.copy(timestamp = Some(dt))
        }

      override def unlift(t: DeltaCombine): Delta = {
        val monoidBothL = MonoidGen.gen[Both[Long]]
        t match {
          case DeltaCombine(Some(x), None, None, None, None, None, toNull) =>
            x.copy(toNull = monoidBothL.combine(x.toNull, toNull))
          case DeltaCombine(None, Some(x), None, None, None, None, toNull) =>
            x.copy(toNull = monoidBothL.combine(x.toNull, toNull))
          case DeltaCombine(None, None, Some(x), None, None, None, toNull) =>
            x.copy(toNull = monoidBothL.combine(x.toNull, toNull))
          case DeltaCombine(None, None, None, Some(x), None, None, toNull) =>
            x.copy(toNull = monoidBothL.combine(x.toNull, toNull))
          case DeltaCombine(None, None, None, None, Some(x), None, toNull) =>
            x.copy(toNull = monoidBothL.combine(x.toNull, toNull))
          case DeltaCombine(None, None, None, None, None, Some(x), toNull) =>
            x.copy(toNull = monoidBothL.combine(x.toNull, toNull))
          case _ => t
        }
      }
    }
}*/

object MonoidUtils {
  val describeMonoid: Monoid[Describe]           = MonoidGen.gen[Describe]
  val bothDescribeMonoid: Monoid[Both[Describe]] = MonoidGen.gen

  val parkaResultMonoid: Monoid[ParkaResult] = MonoidGen.gen[ParkaResult]

}

object Parka {

  private val keyValueSeparator = "§"

  def describe(row: Row)(keys: Set[String]): Map[String, Describe] = {
    val fields = row.asInstanceOf[GenericRowWithSchema].schema.fieldNames

    fields
      .filterNot(keys)
      .map(name => name -> Describe(row.getAs[Any](name)))
      .toMap
  }

  /**
    * @param row            Row from the one of the two Dataset
    * @param side           Right if the row come from the right Dataset otherwise Left
    * @param keys           Column's names of both Datasets that are considered as keys
    * @return               Outer information from one particular row
    */
  def outer(row: Row, side: Side)(keys: Set[String]): Outer = {
    val emptyDescribe: Describe = MonoidUtils.describeMonoid.empty
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
  }

  /**
    * @param left           Row from the left Dataset
    * @param right          Row from the right Dataset
    * @param keys           Column's names of both Datasets that are considered as keys
    * @return               Inner information about comparison between left and right
    */
  def inner(left: Row, right: Row)(keys: Set[String]): Inner = {

    val schema = left.asInstanceOf[GenericRowWithSchema].schema

    val byNames: Map[String, DeltaV2] =
      schema.fieldNames
        .filterNot(keys)
        .map(name => {
          /*val delta: Delta = (left.getAs[Any](name), right.getAs[Any](name)) match {
            case (x: Long, y: Long)           => Delta.apply(x, y)
            case (null, y: Long)              => Delta.apply(null, y)
            case (x: Long, null)              => Delta.apply(x, null)
            case (x: Double, y: Double)       => Delta.apply(x, y)
            case (null, y: Double)            => Delta.apply(null, y)
            case (x: Double, null)            => Delta.apply(x, null)
            case (x: String, y: String)       => Delta.apply(x, y)
            case (null, y: String)            => Delta.apply(null, y)
            case (x: String, null)            => Delta.apply(x, null)
            case (x: Boolean, y: Boolean)     => Delta.apply(x, y)
            case (null, y: Boolean)           => Delta.apply(null, y)
            case (x: Boolean, null)           => Delta.apply(x, null)
            case (x: Date, y: Date)           => Delta.apply(x, y)
            case (null, y: Date)              => Delta.apply(null, y)
            case (x: Date, null)              => Delta.apply(x, null)
            case (x: Timestamp, y: Timestamp) => Delta.apply(x, y)
            case (null, y: Timestamp)         => Delta.apply(null, y)
            case (x: Timestamp, null)         => Delta.apply(x, null)
          }*/
          val delta: DeltaV2 = DeltaV2(left.getAs[Any](name), right.getAs[Any](name))
          name -> delta
        })
        .toMap

    val isEqual            = byNames.forall(_._2.nEqual == 1)
    val nDiff: Seq[String] = if (isEqual) Nil else byNames.filter(_._2.nNotEqual == 1).keys.toSeq.sorted
    Inner(if (isEqual) 1 else 0, if (isEqual) 0 else 1, Map(nDiff -> 1), byNames)
  }

  private val emptyInner: Inner = MonoidGen.empty[Inner]
  private val emptyOuter: Outer = MonoidGen.empty[Outer]

  /**
    * @param left           Row from the left Dataset for a particular key
    * @param right          Row from the right Dataset for a particular key
    * @param keys           Column's names of both Datasets that are considered as keys
    * @return               ParkaResult containing outer or inner information for a key
    */
  def result(left: Iterable[Row], right: Iterable[Row])(keys: Set[String]): ParkaResult =
    (left, right) match {
      //Only  Right
      case (l, r) if l.isEmpty && r.nonEmpty => ParkaResult(emptyInner, outer(r.head, Right)(keys))
      //Only Left
      case (l, r) if l.nonEmpty && r.isEmpty => ParkaResult(emptyInner, outer(l.head, Left)(keys))
      //Inner
      case (l, r) if l.nonEmpty && r.nonEmpty => ParkaResult(inner(l.head, r.head)(keys), emptyOuter)
    }

  def combine(left: ParkaResult, right: ParkaResult): ParkaResult = MonoidUtils.parkaResultMonoid.combine(left, right)

  /**
    * Entry point of Parka
    *
    * @param leftDs         Left Dataset
    * @param rightDs        Right Dataset
    * @param keyNames       Column's names of both Datasets that are considered as keys
    * @return               Delta QA analysis between leftDs and rightDS
    */
  def apply(leftDs: Dataset[_], rightDs: Dataset[_])(keyNames: String*): ParkaAnalysis = {
    assert(keyNames.nonEmpty, "you must have at least one key")
    assert(leftDs.schema == rightDs.schema, "schemas are not equal : " + leftDs.schema + " != " + rightDs.schema)

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
