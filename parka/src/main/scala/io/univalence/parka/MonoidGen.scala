package io.univalence.parka

import cats.kernel.{ Monoid, Semigroup }
import com.twitter.algebird.{ QTree, QTreeSemigroup }

object MonoidGen {
  def empty[T: Monoid]: T = Monoid[T].empty

  def apply[T](_empty: T, _combine: (T, T) => T): Monoid[T] = new Monoid[T] {
    override def combine(x: T, y: T): T = _combine(x, y)
    override def empty: T               = _empty
  }

  implicit val enumMonoid: Monoid[StringEnum] = new Typeclass[StringEnum] {
    override def empty: StringEnum = SmallStringEnum(Map.empty)

    override def combine(x: StringEnum, y: StringEnum): StringEnum =
      (x, y) match {
        case (x: SmallStringEnum, y: SmallStringEnum) =>
          SmallStringEnum(mapMonoid[String, Long].combine(x.data, y.data))
        case _ => LargeStringEnum(Enum.Sketch.MONOID.combine(x.toLargeStringEnum.sketch, y.toLargeStringEnum.sketch))
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

  implicit def mapMonoid[K, V: Monoid]: Monoid[Map[K, V]] =
    MonoidGen(
      Map.empty,
      (m1, m2) => {
        (m1.keySet ++ m2.keySet)
          .map(
            k =>
              k -> (m1.get(k) match {
                case None => m2.getOrElse(k, Monoid[V].empty)
                case Some(x) =>
                  m2.get(k) match {
                    case Some(y) => Monoid[V].combine(x, y)
                    case None    => x
                  }
              })
          )
          .toMap
      }
    )

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
