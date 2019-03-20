package io.univalence.centrifuge

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Result[+T](
  value:       Option[T],
  annotations: Vector[Annotation]
) {

  def addPathPart(s: String): Result[T] =
    mapAnnotations(x => x.copy(onField = x.onField.map(s + _).orElse(Some(s))))

  def mapAnnotations(f: Annotation => Annotation): Result[T] =
    Result(value, annotations.map(f))

  def prependAnnotations(xs: Vector[AnnotationSql]): Result[T] =
    Result(value, xs ++ annotations)

  def isEmpty: Boolean = value.isEmpty

  def isPure: Boolean = value.isDefined && annotations.isEmpty

  def hasAnnotations: Boolean = annotations.nonEmpty

  def map[U](f: T => U): Result[U] = Result(value.map(f), annotations)

  def map2[U, V](result: Result[U])(f: (T, U) => V): Result[V] =
    Result((value, result.value) match {
      case (Some(t), Some(u)) => Some(f(t, u))
      case _                  => None
    }, annotations ++ result.annotations)

  def flatMap[U](f: T => Result[U]): Result[U] =
    value match {
      case None => this.asInstanceOf[Result[U]]
      case Some(v) =>
        val r = f(v)
        r.copy(annotations = annotations ++ r.annotations)
    }

  def filter(f: T => Boolean): Result[T] = Result(value.filter(f), annotations)

  def get: T =
    value.getOrElse(throw new Exception("empty result : " + annotations.mkString("|")))

  def toTry: Try[T] = Try(get)

  def toEither: Either[Vector[Annotation], T] =
    value.fold(Left(annotations).asInstanceOf[Either[Vector[Annotation], T]])(Right(_))
}

object Result {

  def fromTry[T](tr: Try[T])(expToString: Throwable => String): Result[T] =
    tr match {
      case Success(t) => pure(t)
      case Failure(e) => fromError(expToString(e))
    }

  def fromEither[L, R](either: Either[L, R])(leftToString: L => String): Result[R] =
    either.fold(leftToString.andThen(fromError), pure)

  def pure[T](t: T): Result[T] =
    Result(
      value       = Some(t),
      annotations = Vector.empty
    )

  def fromError(error: String): Result[Nothing] =
    Result(
      value       = None,
      annotations = Vector(Annotation.fromString(msg = error, error = true))
    )

  def fromWarning[T](t: T, warning: String): Result[T] =
    Result(
      value       = Some(t),
      annotations = Vector(Annotation.fromString(msg = warning, error = false))
    )
}

case class Annotation(
  message:    String,
  onField:    Option[String] = None,
  fromFields: Vector[String] = Vector.empty,
  isError:    Boolean,
  count:      Long = 1L
) {

  def this(message: String, onField: Option[String], fromFields: Seq[String], isError: Boolean, count: Long) = {
    this(message, onField, fromFields.toVector, isError, count)
  }
}

object Annotation {
  def fromString(
    msg:   String,
    error: Boolean
  ): Annotation =
    Annotation(
      message = msg,
      isError = error,
      count   = 1L
    )

  def missingField(fieldName: String): Annotation =
    Annotation(message = "MISSING_VALUE", onField = Some(fieldName), isError = true)
}
