package io.univalence.plumbus.internal
import org.apache.spark.sql.Row

trait CleanFromRow[A] extends Serializable {
  def clean(a: Any): A
}

object CleanFromRow {
  import magnolia._
  import scala.reflect.ClassTag
  import language.experimental.macros

  type Typeclass[T] = CleanFromRow[T]

  private def instance[A]: Typeclass[A] =
    new Typeclass[A] {
      override def clean(a: Any): A = a.asInstanceOf[A]
    }

  // Instances to associate clean operation to basic types
  implicit val double: Typeclass[Double] = instance
  implicit val boolean: Typeclass[Boolean] = instance
  implicit val strCFR: Typeclass[String] = instance
  implicit val intCFR: Typeclass[Int] = instance
  implicit val longCFR: Typeclass[Long] = instance
  // add other typeclass instances for basic types...

  // Instance for Option type
  implicit def opt[T: Typeclass: Manifest]: Typeclass[Option[T]] =
    new Typeclass[Option[T]] {
      // this helps to avoid type erasure warning
      private val rc = implicitly[Manifest[T]].runtimeClass

      override def clean(a: Any): Option[T] =
        a match {
          case ox: Option[_]
              if ox.forall(x => rc.isAssignableFrom(x.getClass)) =>
            ox.asInstanceOf[Option[T]]
          case null => None
          case x    => Option(implicitly[Typeclass[T]].clean(x))
        }
    }

  // Instance for Seq type
  implicit def seq[T: Typeclass: Manifest]: Typeclass[Seq[T]] = {
    new Typeclass[Seq[T]] {
      // this helps to avoid type erasure warning
      private val rc = implicitly[Manifest[T]].runtimeClass

      override def clean(a: Any): Seq[T] =
        a match {
          case Nil => Nil
          case xs: Seq[_]
              if xs.forall(x => rc.isAssignableFrom(x.getClass)) =>
            xs.asInstanceOf[Seq[T]]
          case x: Seq[_] => x.map(implicitly[Typeclass[T]].clean)
        }
    }
  }

  // Instance generator for case classes
  def combine[T: ClassTag](ctx: CaseClass[CleanFromRow, T]): Typeclass[T] =
    new Typeclass[T] {
      override def clean(a: Any): T =
        a match {
          case a: T => a
          case r: Row =>
            val values: Seq[Any] =
              r.toSeq
                .zip(ctx.parameters)
                .map {
                  case (rowValue, param) => param.typeclass.clean(rowValue)
                }
            ctx.rawConstruct(values)
        }
    }

  implicit def gen[T]: CleanFromRow[T] = macro Magnolia.gen[T]

}
