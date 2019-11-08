package io.univalence.parka

import cats.kernel.{ Monoid, Semigroup }
import com.twitter.algebird.{ QTree, QTreeSemigroup }

object MonoidGen {
  def empty[T: Monoid]: T = Monoid[T].empty

  def apply[T](_empty: T, _combine: (T, T) => T): Monoid[T] = new Monoid[T] {
    override def combine(x: T, y: T): T = _combine(x, y)
    override def empty: T               = _empty
  }

  implicit val enumMonoid: Monoid[Enum] = new Typeclass[Enum] {
    override def empty: Enum = SmallEnum(Map.empty)

    override def combine(x: Enum, y: Enum): Enum =
      (x, y) match {
        case (x: SmallEnum, y: SmallEnum) =>
          val res = SmallEnum(mapMonoid3[EnumKey, Long].combine(x.data, y.data))
          if (res.data.size > Enum.Sketch.HEAVY_HITTERS_COUNT) res.toLargeStringEnum else res
        case _ => LargeEnum(Enum.Sketch.MONOID.combine(x.toLargeStringEnum.sketch, y.toLargeStringEnum.sketch))
      }

  }

  implicit val histogramMonoid: Monoid[Histogram] = new Monoid[Histogram] {
    override def empty: Histogram = Histogram.empty

    private val smallHistogramD = MonoidGen.gen[SmallHistogramD]
    private val smallHistogramL = MonoidGen.gen[SmallHistogramL]

    private val largeHistogram = MonoidGen.gen[LargeHistogram]

    override def combine(x: Histogram, y: Histogram): Histogram =
      (x, y) match {
        case (l: SmallHistogramD, r: SmallHistogramD) =>
          val histo = smallHistogramD.combine(l, r)

          if (histo.values.size > 256)
            histo.toLargeHistogram
          else
            histo

        case (l: SmallHistogramL, r: SmallHistogramL) =>
          val histo = smallHistogramL.combine(l, r)

          if (histo.values.size > 256)
            histo.toLargeHistogram
          else
            histo

        case _ => largeHistogram.combine(x.toLargeHistogram, y.toLargeHistogram)
      }
  }

  implicit def rowBasedMapMonoid[K, V: Monoid]: Monoid[RowBasedMap[K, V]] = new Monoid[RowBasedMap[K, V]] {
    override def empty: RowBasedMap[K, V] = RowBasedMap.empty

    override def combine(x: RowBasedMap[K, V], y: RowBasedMap[K, V]): RowBasedMap[K, V] = x.combine(y)
  }

  implicit def mapMonoid3[K, V: Monoid]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    val valueMonoid: Monoid[V]    = Monoid[V]
    override def empty: Map[K, V] = RowBasedMap.empty

    @inline
    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] =
      (x, y) match {
        case (cx: RowBasedMap[K, V], cy: RowBasedMap[K, V]) => cx.combine(cy)
        case _ =>
          val m1k = x.keySet
          x.map({
            case (k, v) =>
              (k, y.get(k).fold(v)(v2 => Monoid[V].combine(v, v2)))
          }) ++ y.filterKeys(k => !m1k(k))

      }
  }

  implicit object longMonoid extends Monoid[Long] {
    @inline
    final override def empty: Long = 0L

    @inline
    final override def combine(x: Long, y: Long): Long = x + y
  }

  implicit def optionMonoid[T: Semigroup]: Monoid[Option[T]] = new Monoid[Option[T]] {
    private val semi = Semigroup[T]

    override def empty: Option[T] = None

    override def combine(x: Option[T], y: Option[T]): Option[T] =
      (x, y) match {
        case (None, _) => y
        case (_, None) => x
        case (Some(xv), Some(yv)) =>
          Some(semi.combine(xv, yv))
      }
  }

  //The monoid on Option[QTree[Unit]]
  implicit lazy val qTreeSemigroup: Semigroup[QTree[Unit]] = new QTreeSemigroup[Unit](7)

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

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  object MonoidUtils {
    val describeMonoid: Monoid[Describe]           = MonoidGen.gen[Describe]
    val bothDescribeMonoid: Monoid[Both[Describe]] = MonoidGen.gen

    val parkaResultMonoid: Monoid[ParkaResult] = MonoidGen.gen[ParkaResult]
  }
}
