package io.univalence.sparktest

import io.univalence.typedpath.{ ArrayPath, FieldPath, Path, PathOrRoot, Root }
import org.apache.spark.sql.types.{ ArrayType, DataType, StructField, StructType }

import scala.util.{ Failure, Try }

object SchemaComparison {

  sealed trait FieldModification

  final case class AddField(dataType: DataType) extends FieldModification

  final case class RemoveField(dataType: DataType) extends FieldModification

  final case class ChangeFieldType(from: DataType, to: DataType) extends FieldModification

  //final case object SetNullable extends FieldModification

  //final case object SetNonNullable extends FieldModification

  final case class SchemaModification(path: Path, fieldModification: FieldModification)

  /**
    * Compare two schemas returning differences between both
    *
    * Example :
    * val sc1 = struct("number" -> int, "name" -> int)
    * val sc2 = struct("rebmun" -> int, "name" -> double)
    * compareSchema(sc1, sc2)
    *
    * Result :
    * Seq(
    *   SchemaModification(path"number", RemoveField(int)),
    *   SchemaModification(path"name", ChangeFieldType(int, double)),
    *   SchemaModification(path"rebmun", AddField(int))
    * )
    *
    * @param sc1 The entry schema
    * @param sc2 Schema to compare to
    * @return    Rhe sequence of schema modification from sc1 to s2
    */
  def compareSchema(sc1: StructType, sc2: StructType): Seq[SchemaModification] = {

    def compareSchema(sc1: StructType, sc2: StructType, prefix: PathOrRoot): Seq[SchemaModification] = {
      def compareDataType(d1: DataType, d2: DataType, prefix: Path): Seq[SchemaModification] =
        (d1, d2) match {
          case (a, b) if a == b                 => Nil
          case (s1: StructType, s2: StructType) => compareSchema(s1, s2, prefix)
          case (a1: ArrayType, a2: ArrayType)   => compareDataType(a1.elementType, a2.elementType, ArrayPath(prefix))
          case (a, b)                           => Seq(SchemaModification(prefix, ChangeFieldType(a, b)))
        }

      val allFields = (sc1.fieldNames ++ sc2.fieldNames).distinct

      for {
        fieldname <- allFields

        left: Option[StructField]  = sc1.fields.find(_.name == fieldname)
        right: Option[StructField] = sc2.fields.find(_.name == fieldname)
        path: Path                 = FieldPath(FieldPath.createName(fieldname).get, prefix)

        modifications: Seq[SchemaModification] = (left, right) match {
          case (Some(l), None)    => Seq(SchemaModification(path, RemoveField(l.dataType)))
          case (None, Some(r))    => Seq(SchemaModification(path, AddField(r.dataType)))
          case (None, None)       => Nil
          case (Some(l), Some(r)) => compareDataType(l.dataType, r.dataType, path)
        }

        modification <- modifications
      } yield modification
    }

    compareSchema(sc1, sc2, Root)
  }

  case class ApplyModificationErrorWithSource(error: ApplyModificationError,
                                              sc: StructType,
                                              schemaModification: SchemaModification)
      extends Exception
  sealed trait ApplyModificationError extends Exception
  case class DuplicatedField(name: String) extends ApplyModificationError
  case class NotFoundField(name: String) extends ApplyModificationError

  /**
    * Apply a modification to a schema
    *
    * Example :
    * val sc = struct("number" -> int)
    * val sm = SchemaModification(path"rebmun", AddField(int))
    * modifySchema(sc, sm)
    *
    * Result :
    * Success(struct("number" -> int, "rebmun" -> int)
    *
    * @param sc                  The entry schema
    * @param schemaModification  A schema modication that must be applied to the entry schema
    * @return                    The schema after the modification
    */
  def modifySchema(sc: StructType, schemaModification: SchemaModification): Try[StructType] = {

    val res = Try {
      schemaModification match {
        case SchemaModification(path, AddField(dt)) =>
          path match {
            case FieldPath(name, Root) if !sc.fieldNames.contains(name) =>
              StructType(sc.fields :+ StructField(name, dt))
            case FieldPath(name, Root) if sc.fieldNames.contains(name) => throw DuplicatedField(name)
          }
        case SchemaModification(path, ChangeFieldType(from, to)) =>
          path match {
            case FieldPath(name, Root) if sc.fieldNames.contains(name) =>
              StructType(sc.map(field => if (field.name == name) field.copy(dataType = to) else field))
            case FieldPath(name, Root) if !sc.fieldNames.contains(name) => throw NotFoundField(name)

          }
        case SchemaModification(path, RemoveField(_)) =>
          path match {
            case FieldPath(name, Root) if sc.fieldNames.contains(name) =>
              StructType(sc.filter(field => field.name != path.toString))
            case FieldPath(name, Root) if !sc.fieldNames.contains(name) => throw NotFoundField(name)
          }
      }
    }

    res.recoverWith({
      case a: ApplyModificationError => {
        val error = ApplyModificationErrorWithSource(a, sc, schemaModification)
        println(error)
        Failure(error)
      }
    })

  }


}
