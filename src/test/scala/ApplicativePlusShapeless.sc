import shapeless._
import shapeless.labelled._
import shapeless.syntax.singleton
import shapeless.syntax.singleton._

import scala.language.higherKinds

trait Applicative[M[_]] {
  def point[A](a: A): M[A]
  def ap[A, B](m1: M[A])(m2: M[A ⇒ B]): M[B]
  //derived
  def map[A, B](m1: M[A])(f: A ⇒ B): M[B] = ap(m1)(point(f))
  def apply[A, B, C](m1: M[A], m2: M[B])(f: (A, B) ⇒ C): M[C] =
    ap(m2)(map(m1)(f.curried))

}

case class Ahoy(name: String, y: Int)

val oName: Option[String] = Option("hello") // None
val oY:    Option[String] = None

object f extends Poly1 {
  implicit def kv[K, V, A[_]](implicit app: Applicative[A]): Case.Aux[
    FieldType[K, A[V]],
    A[FieldType[K, V]]
  ] =
    at(a ⇒ app.map(a.asInstanceOf[A[V]])(field[K](_)))
}

val a = 'name ->> Option("hello") :: 'y ->> Option(1) :: HNil
val b = Option('name ->> "hello") :: Option('y ->> 1) :: HNil
def assertTypedEquals[A](expected: A, actual: A): Unit =
  assert(expected == actual)
val kName = Witness.`'name`
val kY    = Witness.`'y`
type Res =
  Option[FieldType[kName.T, String]] :: Option[FieldType[kY.T, Int]] :: HNil
implicit val optionAppInstance: Applicative[Option] = new Applicative[Option] {
  override def point[A](a: A): Option[A] = Option(a)

  override def ap[A, B](m1: Option[A])(m2: Option[(A) ⇒ B]): Option[B] =
    m2.flatMap(f ⇒ m1.map(f))
}

a.map(f)
trait Apply2[FH, OutT] {
  type Out
  def apply(fh: FH, outT: OutT): Out
}

object Apply2 {
  type Aux[FH, OutT, Out0] = Apply2[FH, OutT] { type Out = Out0 }

  implicit def apply2[F[_], H, T <: HList](
      implicit
      app: Applicative[F]): Aux[F[H], F[T], F[H :: T]] =
    new Apply2[F[H], F[T]] {
      type Out = F[H :: T]

      def apply(fh: F[H], outT: F[T]): Out = app.apply(fh, outT) {
        _ :: _
      }
    }
}

trait Sequencer[L <: HList] {
  type Out
  def apply(in: L): Out
}

object Sequencer {
  type Aux[L <: HList, Out0] = Sequencer[L] { type Out = Out0 }

  implicit def consSequencerAux[FH, FT <: HList, OutT](implicit
                                                       st: Aux[FT,    OutT],
                                                       ap: Apply2[FH, OutT]): Aux[FH :: FT, ap.Out] =
    new Sequencer[FH :: FT] {
      type Out = ap.Out
      def apply(in: FH :: FT): Out =
        ap(in.head, st(in.tail)) // un.TC.apply2(un(in.head), st(in.tail)) { _ :: _ }
    }

  implicit def nilSequencerAux[F[_]: Applicative]: Aux[HNil, F[HNil]] =
    new Sequencer[HNil] {
      type Out = F[HNil]
      def apply(in: HNil): F[HNil] = the[Applicative[F]].point(HNil: HNil)
    }
}

val sequenced = the[Sequencer[Res]].apply(b)
val generic   = the[LabelledGeneric[Ahoy]]

the[Applicative[Option]].map(sequenced)(generic.from)

def zip[A[_], X, Y](x: A[X])(y: A[Y])(implicit app: Applicative[A]) =
  app.apply(x, y)((x, y) ⇒ (x, y))

zip(Option("A"))(zip(Option("B"))(zip(Option("C"))(Some(null))))

/*
def build[A[_]](name:A[String],y:A[Int])(implicit app:Applicative[A]) = {

  val mapped = ('name ->> name :: 'y ->> y :: HNil).map(f)

  app.map(the[Sequencer[mapped.type]].apply(mapped))(generic.from)
}


build(Option("A"),Option(2))
 */

//assertTypedEquals[Res](b,a.map(f))
