package io.univalence.plumbus

import io.univalence.plumbus.internal.CleanFromRow
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Project

import scala.reflect.runtime.universe.TypeTag

object functions {

  implicit class RicherColumn[C <: Column](column: C) {
    def |>[A: CleanFromRow: TypeTag, B: TypeTag: Encoder](f: A => B): TypedColumn[Any, B] = {
      val f0 = udf[B, A](a => f(serializeAndCleanValue(a)))
      f0(column).as[B]
    }
  }

  implicit class RicherTypedColumnSeq[T, A](typedColumn: TypedColumn[T, Seq[A]]) {
    def map[B](f:                   A => B)(implicit cleanFromRow: CleanFromRow[A],
                          typeTagB: TypeTag[B],
                          typeTag:  TypeTag[A],
                          encoder:  Encoder[Seq[B]]): TypedColumn[Any, Seq[B]] = {
      val f0 = udf[Seq[B], Seq[A]](serializeAndClean(s => s.map(f)))
      f0(typedColumn).as[Seq[B]]
    }

    def filter(p:                             A => Boolean)(implicit encoder: Encoder[Seq[A]],
                                typeTag:      TypeTag[A],
                                cleanFromRow: CleanFromRow[A]): TypedColumn[Any, Seq[A]] = {
      val p0 = udf[Seq[A], Seq[A]](serializeAndClean(s => s.filter(p)))
      p0(typedColumn).as[Seq[A]]
    }

    def flatMap[B](f:                            A => Seq[B])(implicit encoder: Encoder[Seq[B]],
                                   typeTagA:     TypeTag[A],
                                   typeTag:      TypeTag[B],
                                   cleanFromRow: CleanFromRow[A]): TypedColumn[Any, Seq[B]] = {
      val f0 = udf[Seq[B], Seq[A]](serializeAndClean(s => s.flatMap(f)))

      f0(typedColumn).as[Seq[B]]
    }
  }

  implicit class RicherTypedColumn[T, A](typedColumn: TypedColumn[T, A]) {
    def |>[B](f:                   A => B)(implicit cleanFromRow: CleanFromRow[A],
                         typeTag:  TypeTag[B],
                         typeTagA: TypeTag[A],
                         encoder:  Encoder[B]): TypedColumn[Any, B] = {
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
        val fExt0:    Seq[A] => B = fExt.get
        val cleaner0: A => A      = cleaner.get

        fExt0(values.map(cleaner0))
      }
  }

  def coalesceColwithSameName(df: DataFrame): DataFrame = {
    // to be sure its a Project
    val frame = df.select("*")
    // get all expressions: id#3, name#4, age#5, id#7, name#8, age#9
    val cols: Seq[NamedExpression] =
      frame.queryExecution.analyzed.asInstanceOf[Project].projectList
    // output columns
    val outpuCols = frame.columns.distinct

    val colMap: Map[String, Column] = {
      val nameExpressionsByName: Map[String, Seq[NamedExpression]] = cols.groupBy(_.name)

      nameExpressionsByName.map {
        // there is only one expression for this name
        case (name, Seq(expr)) => (name, new Column(expr).as(name))
        // there are many expressions for this name => merge them with coalesce
        case (name, nameExpressions) => (name, coalesce(nameExpressions.map(x => new Column(x)): _*).as(name))
      }
    }

    frame.select(outpuCols.map(colMap): _*)
  }

  def renameColumnsWithSameName(df: DataFrame): DataFrame = {
    val frame            = df.select("*")
    val namedExpressions = frame.queryExecution.analyzed.asInstanceOf[Project].projectList
    val duplicateNames: Set[String] =
      namedExpressions
        .map(_.name)
        .groupBy(identity)
        .filter(_._2.size > 1)
        .keys
        .toSet

    if (duplicateNames.isEmpty) df
    else {
      type NameCount = Map[String, Int]
      type Res       = (NameCount, Seq[Column])

      val zero: Res = (Map.empty, Nil)

      val (_, columns) =
        namedExpressions.foldLeft(zero) {
          case ((counts, cols), exp) =>
            if (duplicateNames(exp.name)) {
              val i   = counts.getOrElse(exp.name, 0)
              val col = new Column(exp).as(exp.name + "_" + i)

              (counts + (exp.name -> (i + 1)), cols :+ col)
            } else {
              (counts, cols :+ new Column(exp))
            }
        }

      df.select(columns: _*)
    }
  }

}
