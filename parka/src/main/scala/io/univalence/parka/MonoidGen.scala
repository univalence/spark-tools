package io.univalence.parka

import cats.kernel.Monoid
import com.twitter.algebird.{ QTree, QTreeSemigroup }

object MonoidGen {

  def empty[T: Monoid]: T = implicitly[Monoid[T]].empty

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
    final override def empty: Long = 0L

    @inline
    final override def combine(x: Long, y: Long): Long = x + y
  }

  implicit object qTreeMonoid extends Monoid[Option[QTree[Unit]]] {
    private val semi = new QTreeSemigroup[Unit](2)
    @inline
    final override def combine(x: Option[QTree[Unit]], y: Option[QTree[Unit]]): Option[QTree[Unit]] = (x, y) match {
      case (None, _) => y
      case (_, None) => x
      case (Some(xv), Some(yv)) =>
        Some(semi.plus(xv, yv))
    }

    @inline
    final override def empty: Option[QTree[Unit]] = None
  }

  implicit class MonoidOps[T: Monoid](t: T) {
    def +(t2: T): T = Monoid[T].combine(t, t2)
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
