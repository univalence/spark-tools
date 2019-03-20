package io.univalence.autobuild.struct

import shapeless.labelled._
import shapeless._

import scala.collection.immutable
import scala.language.higherKinds

trait TypeName[T] {
  def name: String
}

object TypeName {
  import scala.reflect.runtime.universe.TypeTag

  implicit def fromTypeTag[T](implicit typeTag: TypeTag[T]): TypeName[T] =
    new TypeName[T] {
      override def name: String = typeTag.tpe.toString
    }
}

trait PathAwareness[T[_]] {
  def injectPrefix[A](prefix: String)(t: T[A]): T[A]
}

object DefaultPathAwareness {
  implicit def defaultPathAwareness[App[_]] = new PathAwareness[App] {
    override def injectPrefix[A](prefix: String)(t: App[A]): App[A] = t
  }
}

trait FieldsNonRecur[L] {
  def fieldnames: List[(String, String)]

}

trait LowPriorityFieldsNonRecur {
  implicit def caseClassFields[F, G](implicit gen: LabelledGeneric.Aux[F, G],
                                     encode:       Lazy[FieldsNonRecur[G]]): FieldsNonRecur[F] =
    new FieldsNonRecur[F] {
      override def fieldnames: List[(String, String)] = encode.value.fieldnames
    }

  implicit def hcon[K <: Symbol, H, T <: HList](
    implicit
    key:        Witness.Aux[K],
    tv:         TypeName[H],
    tailEncode: Lazy[FieldsNonRecur[T]]
  ): FieldsNonRecur[FieldType[K, H] :: T] =
    new FieldsNonRecur[FieldType[K, H] :: T] {
      override def fieldnames: List[(String, String)] =
        (key.value.name, tv.name) :: tailEncode.value.fieldnames
    }
}

object FieldsNonRecur extends LowPriorityFieldsNonRecur {
  implicit def hnil[L <: HNil]: FieldsNonRecur[L] = new FieldsNonRecur[L] {
    override def fieldnames: List[(String, String)] = Nil
  }

  def fieldnames[A](implicit tmr: FieldsNonRecur[A]): Seq[(String, String)] = tmr.fieldnames
}
