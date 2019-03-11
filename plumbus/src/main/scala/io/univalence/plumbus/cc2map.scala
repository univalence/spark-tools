package io.univalence.plumbus

import java.util.UUID

import mercator.Monadic

import scala.util.Try

object cc2map {

  class Prefix private (private val names: String = "") extends AnyVal {
    def rekey(str: String): String =
      str
        .map(s ⇒ if (s.isUpper) "_" + s.toLower else s.toString)
        .mkString

    def add(name: String): Prefix =
      if (names.isEmpty) new Prefix(name)
      else new Prefix(names + "." + rekey(name))

    def toKey: String = names

  }

  object Prefix {
    val empty: Prefix = new Prefix()
  }

  trait ToMap[A] {
    def toMap(a: A, prefix: Prefix = Prefix.empty): Map[String, Any]
  }

  object ToMap {

    import magnolia._
    type Typeclass[T] = ToMap[T]

    private val _instance: ToMap[Any] =
      new ToMap[Any] {
        import scala.collection.immutable.Map.Map1
        override def toMap(t: Any, prefix: Prefix): Map[String, Any] =
          new Map1(prefix.toKey, t)
      }

    def instance[T]: ToMap[T] = _instance.asInstanceOf[ToMap[T]]

    implicit val str: ToMap[String]      = instance
    implicit val int: ToMap[Int]         = instance
    implicit val uuid: ToMap[UUID]       = instance
    implicit val long: ToMap[Long]       = instance
    implicit val boolean: ToMap[Boolean] = instance

    implicit def opt[T](implicit T: ToMap[T]): ToMap[Option[T]] =
      new ToMap[Option[T]] {
        override def toMap(t: Option[T], prefix: Prefix): Map[String, Any] =
          t match {
            case None    ⇒ Map.empty
            case Some(x) ⇒ T.toMap(x, prefix)
          }
      }

    def combine[T](ctx: CaseClass[Typeclass, T]): ToMap[T] =
      new ToMap[T] {
        override def toMap(t: T, prefix: Prefix): Map[String, Any] =
          ctx.parameters
            .flatMap(param ⇒ {
              param.typeclass.toMap(param.dereference(t), prefix.add(param.label))
            })
            .toMap
      }

    def dispatch[T](ctx: SealedTrait[Typeclass, T]): ToMap[T] =
      new ToMap[T] {
        override def toMap(t: T, prefix: Prefix): Map[String, Any] =
          ctx.dispatch(t) { sub ⇒
            sub.typeclass.toMap(sub.cast(t), prefix)
          }
      }

    implicit def gen[T]: ToMap[T] = macro Magnolia.gen[T]

  }

  trait FromMap[A] {
    def fromMap(map: Map[String, Any]): Try[A]
  }

  object FromMap {

    import magnolia._
    type Typeclass[T] = FromMap[T]

    def instance[T]: FromMap[T] = new FromMap[T] {
      override def fromMap(map: Map[String, Any]): Try[T] = Try(map.head._2.asInstanceOf[T])
    }

    implicit val str: FromMap[String]      = instance
    implicit val int: FromMap[Int]         = instance
    implicit val uuid: FromMap[UUID]       = instance
    implicit val long: FromMap[Long]       = instance
    implicit val boolean: FromMap[Boolean] = instance

    implicit def opt[T](implicit T: FromMap[T]): FromMap[Option[T]] =
      new FromMap[Option[T]] {
        override def fromMap(map: Map[String, Any]): Try[Option[T]] =
          if (map.isEmpty) Try(None)
          else {
            T.fromMap(map).map(Option.apply)
          }
      }

    def combine[T](ctx: CaseClass[Typeclass, T]): FromMap[T] =
      new FromMap[T] {
        override def fromMap(map: Map[String, Any]): Try[T] =
          ctx.constructMonadic(param => { param.typeclass.fromMap(map.filterKeys(_ == param.label)) })
      }

    implicit def gen[T]: FromMap[T] = macro Magnolia.gen[T]
  }

  def toMap[CC: ToMap](cc: CC): Map[String, Any] = implicitly[ToMap[CC]].toMap(cc)

  def fromMap[CC: FromMap](map: Map[String, Any]): Try[CC] = implicitly[FromMap[CC]].fromMap(map)

}
