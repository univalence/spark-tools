package io.univalence.sparktest

import io.univalence.sparktest.SchemaComparison.{ AddField, ChangeFieldType, RemoveField, SchemaModification }
import io.univalence.typedpath.{ ArrayPath, FieldPath, Path, PathOrRoot, Root }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ ArrayType, StructField, StructType }
import org.apache.spark.sql.functions.lit

import scala.util.Try

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

  private def cogroup[K, A, B](left: Seq[A], right: Seq[B])(f1: A => K, f2: B => K): Seq[(K, (Seq[A], Seq[B]))] = {
    //TODO find invariants
    val mappedLeft: Map[K, Seq[A]]  = left.groupBy(f1)
    val mappedRight: Map[K, Seq[B]] = right.groupBy(f2)

    val keys = (mappedLeft.keys.toSeq ++ mappedRight.keys.toSeq).distinct
    keys.map(key => (key, (mappedLeft.getOrElse(key, Seq.empty), mappedRight.getOrElse(key, Seq.empty))))
  }

  def compareValue(v1: ObjectValue, v2: ObjectValue): Seq[ObjectModification] = {
    def compareValue(v1: ObjectValue, v2: ObjectValue, prefix: PathOrRoot): Seq[ObjectModification] = {
      def loop(v1: Value, v2: Value, prefix: Path): Seq[ObjectModification] = (v1, v2) match {
        case (c1: AtomicValue, c2: AtomicValue) => compareAtomicValue(c1, c2, prefix)
        case (c1: ArrayValue, c2: ArrayValue)   => compareArrayValue(c1, c2, prefix)
        case (c1: ObjectValue, c2: ObjectValue) => compareValue(c1, c2, prefix)

        case (l, NullValue) => Seq(ObjectModification(prefix, RemoveValue(l)))
        case (NullValue, r) => Seq(ObjectModification(prefix, AddValue(r)))
        case (l, r)         => Seq(ObjectModification(prefix, ChangeValue(l, r))) // Not sure
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

      for {
        (name, (leftField, rightField)) <- cogroup(v1.fields, v2.fields)(_._1, _._1)

        left       = Option(leftField.head._2)
        right      = Option(rightField.head._2)
        path: Path = FieldPath(FieldPath.createName(name).get, prefix)

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

  def compareEqualSchemaDataFrame(df1: DataFrame, df2: DataFrame): Seq[Seq[ObjectModification]] = {
    val rows1 = df1.collect()
    val rows2 = df2.collect()

    rows1.zipAll(rows2, null, null).foldLeft(Seq(): Seq[Seq[ObjectModification]]) {
      case (acc, (curr1, curr2)) =>
        acc :+ compareValue(fromRow(curr1), fromRow(curr2))
    }
  }

  // Move to SparkTest
  def compareDataframe(df1: DataFrame, df2: DataFrame): Seq[Seq[ObjectModification]] = {
    val schemaMods = SchemaComparison.compareSchema(df1.schema, df2.schema)
    if (schemaMods.nonEmpty) {
      val newSchema = schemaMods.foldLeft(df1.schema) {
        case (sc, sm) =>
          val newSchema = SchemaComparison.modifySchema(sc, sm)
          if (newSchema.isFailure) sc
          else newSchema.get
      }
      // In case of failure, return Nil.
      if (!assertSchemaEquals(df2.schema, newSchema, ignoreNullable = true)) Nil
      // Make it so that df1 has the same schema as df2 before comparing the two.
      else {
        val newDf = schemaMods.foldLeft(df1) {
          case (df, sm) =>
            sm match {
              case SchemaModification(p, AddField(d))    => df.withColumn(p.firstName, lit(null).cast(d))
              case SchemaModification(p, RemoveField(_)) => df.drop(p.firstName)
              case SchemaModification(p, ChangeFieldType(_, to)) =>
                df.withColumn(p.firstName, df.col(p.firstName).cast(to)) // ??? Que faire dans ce cas ?
            }
        }
        compareEqualSchemaDataFrame(newDf, df2)
      }
    } else {
      compareEqualSchemaDataFrame(df1, df2)
    }
  }

  def reportErrorDataframeComparison(df1: DataFrame, df2: DataFrame): Unit = {
    val infos = compareDataframe(df1, df2).map(toStringRowModif)
    throw new Exception(infos.take(10).mkString("\n\n"))
  }

  /**
    * From https://github.com/MrPowers/spark-fast-tests/blob/master/src/main/scala/com/github/mrpowers/spark/fast/tests/SchemaComparer.scala
    */
  def assertSchemaEquals(s1: StructType,
                         s2: StructType,
                         ignoreNullable: Boolean    = false,
                         ignoreColumnNames: Boolean = false): Boolean =
    if (s1.length != s2.length) {
      false
    } else {
      val structFields: Seq[(StructField, StructField)] = s1.zip(s2)
      structFields.forall { t =>
        ((t._1.nullable == t._2.nullable) || ignoreNullable) &&
        ((t._1.name == t._2.name) || ignoreColumnNames) &&
        (t._1.dataType == t._2.dataType)
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
        case _ => null
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
