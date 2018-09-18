package io.univalence

import cats.{Eq, Monad}
import io.univalence.centrifuge.Result

import scala.annotation.tailrec

object CatsContrib {

  implicit val resultMonad: Monad[Result] = new Monad[Result] {

    override def pure[A](x: A): Result[A] = Result.pure(x)

    @tailrec
    override def tailRecM[A, B](a: A)(f: (A) ⇒ Result[Either[A, B]]): Result[B] = {
      f(a) match {
        case r if r.isEmpty ⇒ r.asInstanceOf[Result[B]]
        case r @ Result(None, xs) ⇒ r.asInstanceOf[Result[B]]
        case Result(Some(Left(a2)), xs) ⇒
          tailRecM(a2)(f.andThen(x ⇒ x.prependAnnotations(xs)))
        case r @ Result(Some(Right(b)), xs) ⇒ Result(Some(b), xs)
      }

    }

    override def flatMap[A, B](fa: Result[A])(f: (A) ⇒ Result[B]): Result[B] =
      fa.flatMap(f)
  }

  implicit def equResul[T]: Eq[Result[T]] = Eq.fromUniversalEquals

}
