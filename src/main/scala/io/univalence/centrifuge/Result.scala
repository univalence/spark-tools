package io.univalence.centrifuge

import shapeless.tag.@@
import shapeless.the

import scalaz._


case class Result[+T](value: Option[T],
                      annotations: Map[Option[String @@ Path], List[Annotation]])

object Result {

  private implicit def lm[T]:Semigroup[List[T]] = new Semigroup[List[T]] {
    override def append(f1: List[T], f2: => List[T]): List[T] = f1 ++ f2
  }

  private def updateKey[K, V: Semigroup](m: Map[K, V], f: K => K): Map[K, V] =
  //TODO replace with a MapMonoid ?
    m.toSeq.map({ case (k, v) => f(k) -> v }).groupBy(_._1).mapValues(_.map(_._2).reduce((x, y) => the[Semigroup[V]].append(x, y)))

  def updatePath[T](r: Result[T],
                    pathChange: Option[String @@ Path] => Option[String @@ Path]): Result[T] = {
    val Result(value, annotations) = r
    Result(value, updateKey(annotations, pathChange))
  }

  def pure[T](t: T): Result[T] =
    Result(value = Some(t),
      annotations = Map.empty)

  def fromError(error: String) =
    Result(value = None,
      annotations = Map(None -> List(Annotation.fromString(s = error, error = true))))

  def fromWarning[T](t: T, warning: String) =
    Result(value = Some(t),
      annotations = Map(None -> List(Annotation.fromString(s = warning, error = false))))
}

case class Annotation(msg: String,
                      isError: Boolean,
                      count: Long)

object Annotation {
  def fromString(s: String,
                 error: Boolean): Annotation =
    Annotation(msg = s,
      isError = error,
      count = 1L)
}

trait Path