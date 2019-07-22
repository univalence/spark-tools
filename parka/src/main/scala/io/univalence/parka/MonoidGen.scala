package io.univalence.parka

import cats.kernel.{ Monoid, Semigroup }
import com.twitter.algebird.{ QTree, QTreeSemigroup }

object MonoidGen {

  def empty[T: Monoid]: T = Monoid[T].empty

  def apply[T](_empty: T, _combine: (T, T) => T): Monoid[T] = new Monoid[T] {
    override def combine(x: T, y: T): T = _combine(x, y)
    override def empty: T               = _empty
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
    override def empty: Option[T] = None

    override def combine(x: Option[T], y: Option[T]): Option[T] = {
      val semi = Semigroup[T]
      (x, y) match {
        case (None, _) => y
        case (_, None) => x
        case (Some(xv), Some(yv)) =>
          Some(semi.combine(xv, yv))
      }
    }
  }

  //The monoid on Option[QTree[Unit]]
  implicit val qTreeSemigroup: Semigroup[QTree[Unit]] = new QTreeSemigroup[Unit](7)

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
}
