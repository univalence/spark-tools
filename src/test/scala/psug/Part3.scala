package psug.shapeless

import shapeless.contrib.scalaz.Sequencer
import shapeless.labelled._
import shapeless.ops.hlist.Mapper
import shapeless.syntax.singleton._
import shapeless._

import scala.language.higherKinds
import scalaz._
import scalaz.std.option._

object Builder {

  object transFormField extends Poly1 {
    implicit def kv[K, V, A[_]](implicit app: Applicative[A])
      : Case.Aux[FieldType[K, A[V]], A[FieldType[K, V]]] =
      at(a ⇒ app.map(a.asInstanceOf[A[V]])(field[K](_)))
  }

  def build0[In <: HList, Out <: HList](in: In)(
      implicit map: Mapper.Aux[transFormField.type, In, Out]): Out =
    map.apply(in)

  def build1[In <: HList, Out](in: In)(
      implicit sequencer: Sequencer.Aux[In, Out]): Out = sequencer.apply(in)

  def build2[A[_], In, Out](in: A[In])(
      implicit f: Functor[A],
      gen: LabelledGeneric.Aux[Out, In]): A[Out] = f.map(in)(gen.from)

  def build[In <: HList, In1 <: HList, Out1, A[_], In2, Out2](in: In)(
      implicit
      map: Mapper.Aux[transFormField.type, In, In1],
      sequencer: Sequencer.Aux[In1, Out1],
      un: Unpack1[Out1, A, In2],
      f: Functor[A],
      gen: LabelledGeneric.Aux[Out2, In2]): A[Out2] =
    build2(build1(build0(in)).asInstanceOf[A[In2]])

}

case class Yolo(i: Int)

object Test {

  type namek = Witness.`'name`.T

  def assertTypedEquals[A](expected: A): Unit = ()

  val ex = 'name ->> Option("hello")
  assertTypedEquals[FieldType[namek, Option[String]] :: HNil](ex :: HNil)

  assertTypedEquals[Option[FieldType[namek, String]] :: HNil](
    Builder.transFormField.apply('name ->> Option("hello")) :: HNil)

  case class Ahoy(name: String, y: Int)

  import Builder._

  val o1: Option[Ahoy] = build2(
    build1(build0('name ->> Option("hello") :: 'y ->> Option(1) :: HNil)))

  val o2: Option[Ahoy] = build(
    'name ->> Option("hello") :: 'y ->> Option(1) :: HNil)

  val y: Option[Yolo] = build('i ->> Option(1) :: HNil)

  def main(args: Array[String]) {
    //println((y)
  }

}
