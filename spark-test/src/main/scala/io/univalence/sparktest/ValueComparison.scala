package io.univalence.sparktest

import io.univalence.typedpath.Index.{ ArrayIndex, FieldIndex }
import io.univalence.typedpath.{ FieldPath, Index, IndexOrRoot, Root }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ ArrayType, StructType }

import scala.collection.mutable

object ValueComparison {

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

  final case class ObjectModification(index: Index, valueModification: ValueDiff) extends ValueModification

  def termValue(a: Any): TermValue = a match {
    case null    => NullValue
    case None    => NullValue
    case Some(x) => AtomicValue(x)
    case _       => AtomicValue(a)
  }

  /**
    * Transform a row in an ObjectValue component
    *
    * @param r      the row to be converted
    * @return       the ObjectValue's transposition of the row
    */
  def fromRow(r: Row): ObjectValue =
    r match {
      case g: GenericRowWithSchema =>
        val fields = g.schema.fields
        val mappedFields = fields.map(
          x =>
            x.dataType match {
              case ArrayType(_: StructType, _) =>
                (x.name, ArrayValue(g.getAs[Seq[Row]](x.name).map(fromRow): _*))
              case _: StructType =>
                (x.name, fromRow(g.getAs[Row](x.name)))
              case _: ArrayType =>
                (x.name, ArrayValue(g.getAs[Seq[Any]](x.name).map(termValue): _*))
              case _ =>
                (x.name, termValue(g.getAs[Any](x.name)))
          }
        )
        ObjectValue(mappedFields: _*)

    }

  //TODO find invariants
  /**
    * For each key k in left or right, return a resulting Seq that contains a tuple with the list of
    * values for that key in left or right.
    * See RDD's cogroup function for more information
    */
  private def cogroup[K, A, B](left: Seq[A], right: Seq[B])(f1: A => K, f2: B => K): Seq[(K, (Seq[A], Seq[B]))] = {
    val mappedLeft: Map[K, Seq[A]]  = left.groupBy(f1)
    val mappedRight: Map[K, Seq[B]] = right.groupBy(f2)

    val keys = (mappedLeft.keys.toSeq ++ mappedRight.keys.toSeq).distinct
    keys.map(key => (key, (mappedLeft.getOrElse(key, Seq.empty), mappedRight.getOrElse(key, Seq.empty))))
  }

  /**
    * Compare two ObjectValue together, see the function fromRow and ADT about values for more information
    *
    * @param v1     The original ObjectValue
    * @param v2     The Expected ObjectValue
    * @return       Sequence of ObjectModification between v1 and v2
    */
  def compareValue(v1: ObjectValue, v2: ObjectValue): Seq[ObjectModification] = {
    def compareValue(v1: ObjectValue, v2: ObjectValue, prefix: IndexOrRoot): Seq[ObjectModification] = {
      def loop(v1: Value, v2: Value, prefix: Index): Seq[ObjectModification] = (v1, v2) match {
        case (c1: AtomicValue, c2: AtomicValue) => compareAtomicValue(c1, c2, prefix)
        case (c1: ArrayValue, c2: ArrayValue)   => compareArrayValue(c1, c2, prefix)
        case (c1: ObjectValue, c2: ObjectValue) => compareValue(c1, c2, prefix)

        case (l, NullValue) => Seq(ObjectModification(prefix, RemoveValue(l)))
        case (NullValue, r) => Seq(ObjectModification(prefix, AddValue(r)))
        case (l, r)         => Seq(ObjectModification(prefix, ChangeValue(l, r))) // Not sure
      }

      def compareAtomicValue(av1: AtomicValue, av2: AtomicValue, prefix: Index): Seq[ObjectModification] =
        (av1, av2) match {
          case (AtomicValue(c1), AtomicValue(c2)) if c1 == c2 => Nil
          case _                                              => Seq(ObjectModification(prefix, ChangeValue(av1, av2)))
        }

      def compareArrayValue(av1: ArrayValue, av2: ArrayValue, prefix: Index): Seq[ObjectModification] =
        av1.values.zipAll(av2.values, NullValue, NullValue).zipWithIndex.foldLeft(Seq(): Seq[ObjectModification]) {
          case (acc, ((curr1, curr2), index)) =>
            //peut-être qu'il faut faire un index dans les paths : abc.def[0]
            acc ++ loop(curr1, curr2, ArrayIndex(index, prefix))
        }

      for {
        (name, (leftField, rightField)) <- cogroup(v1.fields, v2.fields)(_._1, _._1)

        left        = Option(leftField.headOption.getOrElse((Nil, NullValue))._2)
        right       = Option(rightField.headOption.getOrElse((Nil, NullValue))._2)
        path: Index = FieldIndex(FieldPath.createName(name).get, prefix)

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

  /**
    * Transform a row in String putting modification in first position
    *
    * @param modifications    Sequence of modifications from one row
    * @param row              Row to stringify
    * @return                 Stringified version of the row
    */
  private def toStringRowMods(modifications: Seq[ObjectModification], row: Row): String = {
    def toStringValue[A](value: A): String = value match {
      case values: mutable.WrappedArray[_] =>
        s"[${values.mkString(", ")}]"
      case _ => value.toString
    }
    val modifiedFields = modifications.map(_.index.firstName).distinct

    val otherFields = row.schema.fieldNames.filter(!modifiedFields.contains(_))
    s"dataframe({${(modifiedFields ++ otherFields).map(x => s"$x: ${toStringValue(row.getAs(x))}").mkString(", ")}})"
  }

  /**
    * Transform a both orginal and expected Dataframe's row in String
    *
    * @param modifications    Sequence of modifications from one row
    * @param row1             Row from the original Dataframe
    * @param row2             Row from the expected Dataframe
    * @return                 Stringified version of the sequence of modifications
    */
  def toStringRowsMods(modifications: Seq[ObjectModification], row1: Row, row2: Row): String = {
    val stringifyRow1 = toStringRowMods(modifications, row1)
    val stringifyRow2 = toStringRowMods(modifications, row2)
    s"\n$stringifyRow1\n$stringifyRow2"
  }

  /**
    * Transform a list of modification from one row in String
    *
    * @param modifications    Sequence of modifications from one row
    * @return                 Stringified version of the sequence of modifications
    */
  def toStringModifications(modifications: Seq[ObjectModification]): String = {

    def stringMod(mod: ObjectModification): String = {
      val pwi = mod.index.firstName.toString
      mod.valueModification match {
        case ChangeValue(AtomicValue(from), AtomicValue(to)) =>
          s"in value at $pwi, $to was not equal to $from"
        case AddValue(AtomicValue(value)) =>
          s"in value at $pwi, $value was added"
        case RemoveValue(AtomicValue(value)) =>
          s"in value at $pwi, $value was removed"
        case _ => null
      }
    }

    modifications.map(stringMod).mkString("\n")
  }
}
