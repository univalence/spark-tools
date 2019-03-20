package io.univalence

trait Monoid[T] {

  def zero: T
  def op(t1: T, t2: T): T
}

object Monoid {
  implicit val intMonoid: Monoid[Int] = new Monoid[Int] {
    override def op(t1: Int, t2: Int): Int = t1 + t2
    override def zero: Int = 0
  }

}

trait CompareValue[T] {
  type Out
  def compare(t1: T, t2: T): Out
}

object CompareValue {
  type Aux[T, C] = CompareValue[T] { type Out = C }

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

  implicit val compareValueInt: CompareValue.Aux[Int, Int] =
    new CompareValue[Int] {
      type Out = Int
      override def compare(t1: Int, t2: Int): Int = t1 - t2
    }

  implicit val compareValueString: CompareValue.Aux[String, Int] =
    new CompareValue[String] {
      type Out = Int
      override def compare(t1: String, t2: String): Int = levenshtein(t1, t2)
    }

  implicit val compareValueBoolean: CompareValue.Aux[Boolean, Boolean] =
    new CompareValue[Boolean] {
      override type Out = Boolean
      override def compare(t1: Boolean, t2: Boolean): Boolean =
        !(t1 ^ t2) // XNOR // ==
    }
}

object IntroScala {

  val s = 1

  val f: Int => Int = x => x * x

  val g: Int => Int = { case 1 => 2; case x => x * x }

  def withInput[A, B](f: A => B): A => (B, A) = x => (f(x), x)

  withInput(f)(3)

  def compare[A, B, C](f: A => B)(rs: (B, A))(implicit compareValue: CompareValue.Aux[B, C]): (B, A, C) = {
    val r = f(rs._2)
    (r, rs._2, compareValue.compare(r, rs._1))
  }

  def compareList[A, B, C](f: A => B)(rss: Seq[(B, A)])(
    implicit
    compareValue: CompareValue.Aux[B, C],
    monoid:       Monoid[C]
  ): (Seq[(B, A)], C) = {

    val r: Seq[(B, A, C)] = rss.map(rs => compare(f)(rs))

    (r.map(t => t._1 -> t._2), r.map(_._3).fold(monoid.zero)(monoid.op))
  }

  sealed trait MyList[+T] {
    def map[B](f: T => B): MyList[B]

    def filter(pred: T => Boolean): MyList[T]

    def flatMap[B](f: T => MyList[B]): MyList[B]

    def fold[C](zero: () => C)(f: (T, C) => C): C

    def union[B >: T](myList: MyList[B]): MyList[B]
  }

  object MyList {

    def apply[T](ts: T*): MyList[T] =
      ts.foldRight(MyNil.asInstanceOf[MyList[T]])(MyCons.apply)
  }

  case object MyNil extends MyList[Nothing] {
    override def map[B](f:    (Nothing) => B):       MyList[B]       = this
    override def filter(pred: (Nothing) => Boolean): MyList[Nothing] = this

    override def fold[C](zero: () => C)(f: (Nothing, C) => C): C = zero()

    override def flatMap[B](f: (Nothing) => MyList[B]): MyList[B] = this

    override def union[B >: Nothing](myList: MyList[B]): MyList[B] = myList
  }

  case class MyCons[+T](head: T, tail: MyList[T]) extends MyList[T] {
    override def map[B](f: (T) => B): MyList[B] = MyCons(f(head), tail.map(f))

    override def filter(pred: (T) => Boolean): MyList[T] =
      if (pred(head)) {
        MyCons(head, tail.filter(pred))
      } else tail.filter(pred)

    override def fold[C](zero: () => C)(f: (T, C) => C): C =
      f(head, tail.fold(zero)(f))

    override def flatMap[B](f: (T) => MyList[B]): MyList[B] =
      f(head).union(tail.flatMap(f))

    override def union[B >: T](myList: MyList[B]): MyList[B] =
      MyCons(head, tail.union(myList))
  }

  def main(args: Array[String]): Unit = {

    val c: (Int, Int, Int) = compare(g)(withInput(f)(1))

    def fs(s: Int): String = s.toString

    def gs(s: Int): String = Integer.toHexString(s)

    val r: (String, Int, Int) = compare(gs)(withInput(fs)(10))

    val rs = (0 to 11).map(withInput(fs))
    ////println(((rs)

    ////println(((compareList(gs)(rs))

  }

}
