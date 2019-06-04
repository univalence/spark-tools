package io.univalence.sparktest

import io.univalence.typedpath.{ ArrayPath, FieldPath, Path, PathOrRoot, Root }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ ArrayType, StructType }

sealed trait Value

case class ArrayValue(values: Value*) extends Value
case class ObjectValue(fields: (String, Value)*) extends Value

sealed trait TermValue extends Value
case class AtomicValue(value: Any) extends TermValue
case object NullValue extends TermValue

sealed trait ValueModification

sealed trait ValueDiff extends ValueModification
final case class AddValue(value: Value) extends ValueDiff
final case class RemoveValue(value: Value) extends ValueDiff
final case class ChangeValue(from: Value, to: Value) extends ValueDiff

final case class ObjectModification(path: Path, valueModification: ValueDiff) extends ValueModification

object Value {

  def termValue(a: Any): TermValue = a match {
    case null    => NullValue
    case None    => NullValue
    case Some(x) => AtomicValue(x)
    case _       => AtomicValue(a)
  }

  def fromRow(r: Row): ObjectValue =
    r match {
      case g: GenericRowWithSchema =>
        val fields = g.schema.fields
        val mappedFields = fields.map(
          x =>
            x.dataType match {
              case ArrayType(_: StructType, _) =>
                (x.name, ArrayValue(g.getAs[Seq[Row]](x.name).map(fromRow): _*))
              case st: StructType =>
                (x.name, fromRow(g.getAs[Row](x.name)))
              case at: ArrayType =>
                (x.name, ArrayValue(g.getAs[Seq[Any]](x.name).map(termValue): _*))
              case v =>
                (x.name, termValue(g.getAs[Any](x.name)))
          }
        )
        ObjectValue(mappedFields: _*)

    }

  private def cogroup[K, A, B](left: Seq[A], right: Seq[B])(f1: A => K, f2: B => K): Seq[(K, (Seq[A], Seq[B]))] =
    //TODO implemens + find invariants
    ???

  def compareValue(v1: ObjectValue, v2: ObjectValue): Seq[ObjectModification] = {
    def compareValue(v1: ObjectValue, v2: ObjectValue, prefix: PathOrRoot): Seq[ObjectModification] = {
      def loop(v1: Value, v2: Value, prefix: Path): Seq[ObjectModification] = (v1, v2) match {
        case (c1: AtomicValue, c2: AtomicValue) => compareAtomicValue(c1, c2, prefix)
        case (c1: ArrayValue, c2: ArrayValue)   => compareArrayValue(c1, c2, prefix)
        case (c1: ObjectValue, c2: ObjectValue) => compareValue(c1, c2, prefix)

        case (l, NullValue) => Seq(ObjectModification(prefix, RemoveValue(l)))
        case (NullValue, r) => Seq(ObjectModification(prefix, AddValue(r)))
      }

      def compareAtomicValue(av1: AtomicValue, av2: AtomicValue, prefix: Path): Seq[ObjectModification] =
        (av1, av2) match {
          case (AtomicValue(c1), AtomicValue(c2)) if c1 == c2 => Nil
          case _                                              => Seq(ObjectModification(prefix, ChangeValue(av1, av2)))
        }

      def compareArrayValue(av1: ArrayValue, av2: ArrayValue, prefix: Path): Seq[ObjectModification] =
        av1.values.zipAll(av2.values, NullValue, NullValue).zipWithIndex.foldLeft(Seq(): Seq[ObjectModification]) {
          case (acc, ((curr1, curr2), index)) =>
            //peut-être qu'il faut faire un index dans les paths : abc.def[0]
            acc ++ loop(curr1, curr2, FieldPath(FieldPath.createName("index" + index.toString).get, ArrayPath(prefix)))
        }

      val allFields = (v1.fields.map(_._1) ++ v2.fields.map(_._1)).distinct

      for {
        //TODO : Utiliser un cogroup sur seq
        fieldname <- allFields

        left: Option[Value]  = v1.fields.find(_._1 == fieldname).map(_._2)
        right: Option[Value] = v2.fields.find(_._1 == fieldname).map(_._2)
        path: Path           = FieldPath(FieldPath.createName(fieldname).get, prefix)

        modifications: Seq[ObjectModification] = (left, right) match {
          case (Some(l), None)    => Seq(ObjectModification(path, RemoveValue(l)))
          case (None, Some(r))    => Seq(ObjectModification(path, AddValue(r)))
          case (None, None)       => Nil
          case (Some(l), Some(r)) => loop(l, r, path)
        }

        modification <- modifications
      } yield modification
    }
    compareValue(v1, v2, Root)
  }

  // Move to SparkTest
  def compareDataframe(df1: DataFrame, df2: DataFrame): Seq[Seq[ObjectModification]] = {
    //TODO : Il faudrait vérifier et aligner les schémas avant, on ne peut comparer que le noyau en commun
    val rows1 = df1.collect()
    val rows2 = df2.collect()

    rows1.zipAll(rows2, null, null).foldLeft(Seq(): Seq[Seq[ObjectModification]]) {
      case (acc, (curr1, curr2)) =>
        acc :+ compareValue(fromRow(curr1), fromRow(curr2))
    }

  }

  def toStringRowModif(modifications: Seq[ObjectModification]): String = {
    def getPathWithIndex(path: Path): String =
      path.firstName + (if (path.toString.contains("/index")) " at index " + path.toString.split("/index").last else "")

    def stringMod(mod: ObjectModification): String = {
      val pwi = getPathWithIndex(mod.path)
      mod.valueModification match {
        case ChangeValue(AtomicValue(from), AtomicValue(to)) =>
          s"in field $pwi, $to was not equal to $from"
        case AddValue(AtomicValue(value)) =>
          s"in field $pwi, $value was added"
        case RemoveValue(AtomicValue(value)) =>
          s"in field $pwi, $value was removed"
      }
    }

    modifications.map(stringMod).mkString("\n")
  }

  // Move to SparkTest
  def toStringDataframeModif(seqModifications: Seq[Seq[ObjectModification]]): String = {
    val infos = for {
      modifications <- seqModifications
    } yield toStringRowModif(modifications)

    infos.take(10).mkString("\n\n")
  }
}

//class ValueComparison {}
