package psug.part0

import scala.language.higherKinds

class Part0 {

  /** HELLO PSUG !!!
  *
  * MY NAME IS AHOY-JON
  */
}

trait Functor[F[_]] {
  def map[A, B](fa: F[A])(f: A => B): F[B]
}

trait Applicative[M[_]] extends Functor[M] {
  def point[A](a: A): M[A]

  def zip[A, B](ma: M[A], mb: M[B]): M[(A, B)]

  def ap[A, B](ma: M[A], mb: M[A => B]): M[B] = map(zip(ma, mb))(t => t._2(t._1))

  def apply2[A, B, C](ma: M[A], mb: M[B])(f: (A, B) => C): M[C] =
    ap(mb, map(ma)(x => (b: B) => f(x, b)))
}

trait Monads[M[_]] extends Applicative[M] {
  def flatMap[A, B](ma: M[A])(f: A => M[B]): M[B]
}

case class IronSuit[T](value: T) {
  def map[B](f: T => B): IronSuit[B] = IronSuit(f(value))

  def flatMap[B](f: T => IronSuit[B]): IronSuit[B] = f(value)

}

object Functor {

  implicit val ironSuitInstance: Monads[IronSuit] = new Monads[IronSuit] {
    override def flatMap[A, B](ma: IronSuit[A])(f: (A) => IronSuit[B]): IronSuit[B] = f(ma.value)

    override def point[A](a: A): IronSuit[A] = IronSuit(a)

    override def map[A, B](fa: IronSuit[A])(f: (A) => B): IronSuit[B] =
      IronSuit(f(fa.value))

    override def ap[A, B](ma: IronSuit[A], mb: IronSuit[(A) => B]): IronSuit[B] =
      point(mb.value(ma.value))

    override def zip[A, B](ma: IronSuit[A], mb: IronSuit[B]): IronSuit[(A, B)] =
      ???
  }

  implicit class monadOps[M[_], A](fa: M[A])(implicit monad: Monads[M]) {
    def map[B](f: A => B) = monad.map(fa)(f)

    def flatMap[B](f: A => M[B]): M[B] = monad.flatMap(fa)(f)

    def apply2[B, C](mb: M[B])(f: (A, B) => C): M[C] = monad.apply2(fa, mb)(f)
  }

}

object Test2 {
  def main(args: Array[String]) {

    import Functor._

    //println((for (x ← IronSuit(1); y ← IronSuit("a")) yield (x + y))

    //println((IronSuit(1).apply2(IronSuit("a"))(_ + _))

    //println((Option(1).map(_ + 1))

    //println((Option(-1).flatMap(x ⇒ if (x < 0) None else Some("abc")))

  }
}
