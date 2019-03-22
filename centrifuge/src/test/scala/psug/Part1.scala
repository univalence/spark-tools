package psug

trait Applicative[M[_]] {
  def point[A](a: A): M[A]

  def ap[A, B](m1: M[A])(m2: M[A => B]): M[B]

  // def flatMap[A,B](m1:M[A])(f:A => M[B]):M[B]

  //derived
  def map[A, B](m1: M[A])(f: A => B): M[B] = ap(m1)(point(f))

  def apply2[A, B, C](m1: M[A], m2: M[B])(f: (A, B) => C): M[C] =
    ap(m2)(map(m1)(f.curried))

  def zip[A, B](m1: M[A], m2: M[B]): M[(A, B)] = {
    def tpl(x: A): B => (A, B) = (x, _)

    ap(m2)(map(m1)(tpl))
  }
}

trait ApplicativeAlternative[M[_]] {
  def point[A](a: A): M[A]

  def zip[A, B](m1: M[A], m2: M[B]): M[(A, B)]

  def map[A, B](m1: M[A])(f: A => B): M[B]

  // def flatMap[A,B](m1:M[A])(f:A => M[B]):M[B]
  //derived
  def ap[A, B](m1: M[A])(m2: M[A => B]): M[B] =
    map(zip(m1, m2))(t => t._2(t._1))

  def apply2[A, B, C](m1: M[A], m2: M[B])(f: (A, B) => C): M[C] =
    map(zip(m1, m2))(f.tupled)
}

object IronSuit {
  implicit val applicativeInst: Applicative[IronSuit] =
    new Applicative[IronSuit] {
      override def point[A](a: A): IronSuit[A] = IronSuit(a)

      override def ap[A, B](m1: IronSuit[A])(m2: IronSuit[(A) => B]): IronSuit[B] = IronSuit(m2.value(m1.value))
    }
  implicit val applicativeAltInst: ApplicativeAlternative[IronSuit] =
    new ApplicativeAlternative[IronSuit] {
      override def point[A](a: A): IronSuit[A] = IronSuit(a)

      override def zip[A, B](m1: IronSuit[A], m2: IronSuit[B]): IronSuit[(A, B)] =
        IronSuit((m1.value, m2.value))

      override def map[A, B](m1: IronSuit[A])(f: (A) => B): IronSuit[B] =
        IronSuit(f(m1.value))
    }
}

case class IronSuit[T](value: T)

object Test {

  val is1 = IronSuit(1)
  val isA = IronSuit("A")

  def main(args: Array[String]) {

    implicitly[Applicative[IronSuit]].apply2(is1, isA)(_ + _)

    implicitly[ApplicativeAlternative[IronSuit]].apply2(is1, isA)(_ + _)

  }
}

case class MyValidation[T](nominal: Option[T], err: Option[String])
