package io.univalence

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import scala.util.matching.Regex

sealed trait Prefix
case object PrefixArray extends Prefix
case class PrefixName(name: String) extends Prefix

object SparkLens {

  /*implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ ⇒ "x"): _*)
  }*/

  type Path = Seq[Prefix]

  def pathToStr(path: Path): String =
    path
      .map({
        case PrefixName(name) ⇒ name
        case PrefixArray      ⇒ "[]"
      })
      .mkString("/")

  def lensRegExp(df:                       DataFrame)(fieldSelect: (String, DataType) ⇒ Boolean,
                                transform: (Any, DataType) ⇒ Any): DataFrame = {
    lens(df)({ case (p, dt) ⇒ fieldSelect(pathToStr(p), dt) }, transform)
  }

  type Jump = Seq[Option[Int]]

  def lens(df: DataFrame)(fieldSelect: (Path, DataType) ⇒ Boolean, transform: (Any, DataType) ⇒ Any): DataFrame = {

    val schema = df.schema

    def matchJump(prefix: Jump = Vector.empty, path: Path = Nil, dataType: DataType): Seq[(Jump, DataType)] = {

      val first: Option[(Jump, DataType)] =
        if (fieldSelect(path, dataType)) Some(prefix → dataType) else None

      val recur: Seq[(Jump, DataType)] = dataType match {
        case StructType(fields) ⇒
          fields.zipWithIndex.flatMap({
            case (s, i) ⇒
              val j       = prefix :+ Some(i)
              val newPath = path :+ PrefixName(s.name)

              matchJump(j, newPath, s.dataType)
          })

        case ArrayType(dt, _) ⇒
          val j = prefix :+ None
          matchJump(j, path :+ PrefixArray, dt)
        case _ ⇒ Vector.empty
      }
      first.toSeq ++ recur
    }

    val toTx: Seq[(Jump, DataType)] =
      matchJump(Vector.empty, Vector.empty, schema)

    val res: RDD[Row] = df.rdd.map { gen ⇒
      toTx.foldLeft(gen)({
        case (r, (j, dt)) ⇒
          update(j, r, a ⇒ transform(a, dt)).asInstanceOf[Row]
      })
    }

    df.sparkSession.createDataFrame(res, schema)

  }

  private def update(j: Jump, r: Any, f: Any ⇒ Any): Any = {
    j.toList match {
      case Nil                  ⇒ f(r)
      case x :: xs if r == null ⇒ null
      case None :: xs           ⇒ r.asInstanceOf[Seq[Any]].map(x ⇒ update(xs, x, f))
      case Some(i) :: xs ⇒
        val row = r.asInstanceOf[Row]
        val s   = row.toSeq
        new GenericRow(s.updated(i, update(xs, s(i), f)).toArray)
    }
  }

}

/* //TODO Use a tree
sealed trait JumpTree

case class Root(childs: Seq[BranchTree]) extends JumpTree
case class BranchTree(pos: Option[Int], childs: Seq[BranchTree]) extends JumpTree
 */
