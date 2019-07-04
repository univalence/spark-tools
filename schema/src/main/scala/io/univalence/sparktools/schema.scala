package io.univalence.sparktools.schema

import io.univalence.strings.{FieldKey, KeyOrRoot, Root}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.util.Try

sealed trait StrExp {
  def exp: String
}
final case class SingleExp(exp: String) extends StrExp

final case class StructExp(fieldExps: Seq[(StrExp, String)]) extends StrExp {
  override def exp: String = fieldExps.map(x => x._1.exp + " as " + x._2).mkString("struct(", ", ", ")")
  def asProjection: String = fieldExps.map(x => x._1.exp + " as " + x._2).mkString(", ")
}


object Schema {

  type Tx = Dataset[_] => Try[DataFrame]

  def move(from: KeyOrRoot, to: KeyOrRoot):Tx = {
    (from, to) match {
      case (FieldKey(a,Root), FieldKey(b,Root)) =>
        df => Try(df.withColumnRenamed(a,b))
    }
  }

  case class Point(dt: DataType, ref:String)

  def transformAtPath(target: KeyOrRoot, tx: Point => StrExp): Tx = ???



}



