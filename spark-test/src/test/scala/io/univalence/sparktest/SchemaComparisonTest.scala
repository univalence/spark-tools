package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.scalatest.FunSuite
import SchemaComparison._

import scala.util.{Success, Failure}

class SchemaComparisonTest extends FunSuite with SparkTest {
  import io.univalence.typedpath._

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  test("The two schemas are empty") {
    val sc1 = StructType(List())
    assert(compareSchema(sc1, sc1) == Nil)
  }

  test("Two identical schema have no schema modification") {
    val sc1 = StructType(List(StructField("number", IntegerType)))
    val expectedSchemaMod = Seq.empty
    assert(compareSchema(sc1, sc1) == expectedSchemaMod)
  }
  
  test("A field removed should return a SchemaModification RemoveField") {
    val sc1 = StructType(List(StructField("number", IntegerType), StructField("name", IntegerType)))
    val sc2 = StructType(List(StructField("number", IntegerType)))
    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"name", RemoveField(IntegerType))))
  }

  test("A field added should return a SchemaModification with AddField") {
    val sc1 = StructType(List(StructField("number", IntegerType)))
    val sc2 = StructType(List(StructField("number", IntegerType), StructField("name", IntegerType)))

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"name", AddField(IntegerType))))
  }

  test("A field changed should return a SchemaModification with ChangeField") {
    val sc1 = StructType(List(StructField("number", IntegerType)))
    val sc2 = StructType(List(StructField("number", DoubleType)))

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"number", ChangeFieldType(IntegerType, DoubleType))))
  }
  
  test("A field removed, a field added, and a field changed should return a" +
    "SchemaModification with RemoveField, AddField, and ChangeField") {
    val sc1 = StructType(List(StructField("number", IntegerType), StructField("name", IntegerType)))
    val sc2 = StructType(List(StructField("rebmun", IntegerType), StructField("name", DoubleType)))

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"number",RemoveField(IntegerType)),
      SchemaModification(path"name",ChangeFieldType(IntegerType,DoubleType)), SchemaModification(path"rebmun",AddField(IntegerType)))
    )
  }

  test("Adding a field while the field exists should return a Duplicated Field error") {
    val sc = StructType(List(StructField("number", IntegerType)))
    val sm = SchemaModification(path"number", AddField(IntegerType))

    assert(SchemaComparison.modifySchema(sc, sm) ==
      Failure(ApplyModificationErrorWithSource(DuplicatedField("number"), sc, sm)))
  }

  test("Adding a field while the field not exists should return a Success with the new StructType") {
    val sc = StructType(List(StructField("number", IntegerType)))
    val sm = SchemaModification(path"rebmun", AddField(IntegerType))

    assert(SchemaComparison.modifySchema(sc, sm) ==
      Success(StructType(List(StructField("number", IntegerType), StructField("rebmun", IntegerType)))))
  }

  test("Removing a field while the field is inexistant should return a Not Found Field error") {
    val sc = StructType(List(StructField("number", IntegerType)))
    val sm = SchemaModification(path"name", RemoveField(IntegerType))

    assert(SchemaComparison.modifySchema(sc, sm) ==
      Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm)))
  }

  test("Removing a field while the field is existant should return a Success with the new StructType") {
    val sc = StructType(List(StructField("number", IntegerType), StructField("rebmun", IntegerType)))
    val sm = SchemaModification(path"rebmun", RemoveField(IntegerType))

    assert(SchemaComparison.modifySchema(sc, sm) ==
      Success(StructType(List(StructField("number", IntegerType)))))
  }

  test("Updating a field type while the field is inexistant should return a Not Found Field error") {
    val sc = StructType(List(StructField("number", IntegerType)))
    val sm = SchemaModification(path"name", ChangeFieldType(IntegerType, DoubleType))

    assert(SchemaComparison.modifySchema(sc, sm) ==
      Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm)))
  }

  test("Updating a field type while the field is existant should return a Success with the new StructType") {
    val sc = StructType(List(StructField("number", IntegerType)))
    val sm = SchemaModification(path"number", ChangeFieldType(IntegerType, DoubleType))

    assert(SchemaComparison.modifySchema(sc, sm) ==
      Success(StructType(List(StructField("number", DoubleType)))))
  }
}
