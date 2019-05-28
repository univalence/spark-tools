package io.univalence.sparktest

import io.univalence.typedpath.{ ArrayPath, FieldPath, Path, PathOrRoot, Root }
import org.apache.spark.sql.types.{ ArrayType, DataType, StructField, StructType }

import scala.util.Try

object SchemaComparison {

  sealed trait FieldModification
  final case class AddField(dataType: DataType) extends FieldModification
  final case class RemoveField(dataType: DataType) extends FieldModification
  final case class ChangeFieldType(from: DataType, to: DataType) extends FieldModification
  //final case object SetNullable extends FieldModification
  //final case object SetNonNullable extends FieldModification

  final case class SchemaModification(path: Path, fieldModification: FieldModification)

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

  def modifySchema(sc: StructType, schemaModification: SchemaModification): Try[StructType] = ???

  def invariant(sc1: StructType, sc2: StructType): Boolean = {
    val diff = compareSchema(sc1, sc2)

    (for {
      sc3 <- diff.foldLeft(Try(sc1))((sc, modif) => sc.flatMap(x => modifySchema(x, modif)))
    } yield compareSchema(sc1, sc3).isEmpty) == Try(true)
  }

}
