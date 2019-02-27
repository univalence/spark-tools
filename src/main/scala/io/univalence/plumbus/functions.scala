package io.univalence.plumbus

import io.univalence.plumbus.internal.CleanFromRow
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe.TypeTag

object functions {

  implicit class RicherColumn[C <: Column](column: C) {
    def |>[A: CleanFromRow: TypeTag, B: TypeTag: Encoder](f: A => B): TypedColumn[Any, B] = {
      val f0 = udf[B, A](a => f(serializeAndCleanValue(a)))
      f0(column).as[B]
    }
  }

  implicit class RicherTypedColumnSeq[T, A](typedColumn: TypedColumn[T, Seq[A]]) {
    def map[B](f: A => B)(implicit cleanFromRow: CleanFromRow[A],
                          typeTagB: TypeTag[B],
                          typeTag: TypeTag[A],
                          encoder: Encoder[Seq[B]]): TypedColumn[Any, Seq[B]] = {
      val f0 = udf[Seq[B], Seq[A]](serializeAndClean(s => s.map(f)))
      f0(typedColumn).as[Seq[B]]
    }

    def filter(p: A => Boolean)(implicit encoder: Encoder[Seq[A]],
                                typeTag: TypeTag[A],
                                cleanFromRow: CleanFromRow[A]): TypedColumn[Any, Seq[A]] = {
      val p0 = udf[Seq[A], Seq[A]](serializeAndClean(s => s.filter(p)))
      p0(typedColumn).as[Seq[A]]
    }

    def flatMap[B](f: A => Seq[B])(implicit encoder: Encoder[Seq[B]],
                                   typeTagA: TypeTag[A],
                                   typeTag: TypeTag[B],
                                   cleanFromRow: CleanFromRow[A]): TypedColumn[Any, Seq[B]] = {
      val f0 = udf[Seq[B], Seq[A]](serializeAndClean(s => s.flatMap(f)))

      f0(typedColumn).as[Seq[B]]
    }
  }

  implicit class RicherTypedColumn[T, A](typedColumn: TypedColumn[T, A]) {
    def |>[B](f: A => B)(implicit cleanFromRow: CleanFromRow[A],
                         typeTag: TypeTag[B],
                         typeTagA: TypeTag[A],
                         encoder: Encoder[B]): TypedColumn[Any, B] = {
      val f0 = udf[B, A](a => f(serializeAndCleanValue(a)))
      f0(typedColumn).as[B]
    }
  }

  import com.twitter.chill.Externalizer

  protected[plumbus] def serializeAndCleanValue[A: CleanFromRow](a: A): A =
    if (a == null)
      null.asInstanceOf[A]
    else
      Externalizer(implicitly[CleanFromRow[A]].clean _).get(a)

  protected[plumbus] def serializeAndClean[A: CleanFromRow, B](f: Seq[A] => B): Seq[A] => B = {
    val cleaner: Externalizer[A => A] =
      Externalizer(implicitly[CleanFromRow[A]].clean _)
    val fExt: Externalizer[Seq[A] => B] =
      Externalizer(f)

    values =>
      if (values == null) {
        null.asInstanceOf[B]
      } else {
        val fExt0: Seq[A] => B = fExt.get
        val cleaner0: A => A   = cleaner.get

        fExt0(values.map(cleaner0))
      }
  }

}
