package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import SchemaComparison._
import org.scalacheck.{ Arbitrary, Shrink }
import org.scalatest.prop.PropertyChecks

import scala.util.{ Failure, Success, Try }

object SchemaBuilder {
  def double: DoubleType                            = DoubleType
  def int: IntegerType                              = IntegerType
  def struct(args: (String, DataType)*): StructType = StructType(args.map({ case (k, b) => StructField(k, b) }))
}

class SchemaComparisonTest extends FunSuite with SparkTest with PropertyChecks {
  import io.univalence.typedpath._

  import SchemaBuilder._

  val sharedSparkSession: SparkSession = ss
  val sc: SparkContext                 = ss.sparkContext

  test("The two schemas are empty") {
    val sc1: StructType = struct()
    assert(compareSchema(sc1, sc1) == Nil)
  }

  test("Two identical schema have no schema modification") {
    val sc1 = struct("number" -> int)
    assert(compareSchema(sc1, sc1) == Nil)
  }

  test("A field removed should return a SchemaModification RemoveField") {
    val sc1 = struct("number" -> int, "name" -> int)
    val sc2 = struct("number" -> int)
    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"name", RemoveField(int))))
  }

  test("A field added should return a SchemaModification with AddField") {
    val sc1 = struct("number" -> int)
    val sc2 = struct("number" -> int, "name" -> int)

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"name", AddField(int))))
  }

  test("A field changed should return a SchemaModification with ChangeField") {
    val sc1 = struct("number" -> int)
    val sc2 = struct("number" -> double)

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"number", ChangeFieldType(int, DoubleType))))
  }

  test(
    "A field removed, a field added, and a field changed should return a" +
      "SchemaModification with RemoveField, AddField, and ChangeField"
  ) {
    val sc1 = struct("number" -> int, "name" -> int)
    val sc2 = struct("rebmun" -> int, "name" -> double)

    assert(
      compareSchema(sc1, sc2) == Seq(
        SchemaModification(path"number", RemoveField(int)),
        SchemaModification(path"name", ChangeFieldType(int, double)),
        SchemaModification(path"rebmun", AddField(int))
      )
    )
  }

  test("Adding a field while the field exists should return a Duplicated Field error") {
    val sc = struct("number" -> int)
    val sm = SchemaModification(path"number", AddField(int))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(DuplicatedField("number"), sc, sm))
    )
  }

  test("Adding a field while the field not exists should return a Success with the new StructType") {
    val sc = struct("number" -> int)
    val sm = SchemaModification(path"rebmun", AddField(int))

    assert(
      modifySchema(sc, sm) ==
        Success(struct("number" -> int, "rebmun" -> int))
    )
  }

  test("Removing a field while the field is inexistant should return a Not Found Field error") {
    val sc = struct("number" -> int)
    val sm = SchemaModification(path"name", RemoveField(int))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm))
    )
  }

  test("Removing a field while the field is existant should return a Success with the new StructType") {
    val sc = struct("number" -> int, "rebmun" -> int)
    val sm = SchemaModification(path"rebmun", RemoveField(int))

    assert(
      modifySchema(sc, sm) ==
        Success(struct(("number", int)))
    )
  }

  test("Updating a field type while the field is inexistant should return a Not Found Field error") {
    val sc = struct("number" -> int)
    val sm = SchemaModification(path"name", ChangeFieldType(int, DoubleType))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm))
    )
  }

  test("Updating a field type while the field is existant should return a Success with the new StructType") {
    val sc = struct("number" -> int)
    val sm = SchemaModification(path"number", ChangeFieldType(int, double))

    assert(
      modifySchema(sc, sm) ==
        Success(struct(("number", DoubleType)))
    )
  }

  test("test invariant") {

    def invariant(sc1: StructType, sc2: StructType): Boolean = {
      val diff = compareSchema(sc1, sc2)

      val sc3 = diff.foldLeft(Try(sc1))((sc, modif) => sc.flatMap(x => modifySchema(x, modif))).get

      val delta = compareSchema(sc3, sc2) // Should always be empty

      delta.isEmpty
    }

    implicit val ab: Arbitrary[StructType] = Arbitrary(DatatypeGen.genSchema(5))

    import DatatypeGen._




    forAll { (s1: StructType, s2: StructType) =>
      invariant(s1, s2)
    }

  }

}

object DatatypeGen {
  import org.scalacheck.Gen
  import SchemaBuilder._

  implicit def shrinkStrucType: Shrink[StructType] = Shrink[StructType](
    schema =>
      for {
        n <- (1 until schema.fieldNames.length).toStream
        fields = schema.fields.filter(_.name != schema.fieldNames(n))
        rest <- Shrink.shrinkWithOrig(StructType(fields))
      } yield rest
  )


  def fieldNames: Gen[String] = Gen.alphaLowerChar.map(_.toString)

  def leafDatatype: Gen[DataType] = Gen.oneOf(IntegerType, StringType, DoubleType)

  def genDataType(maxDepth: Int = 20): Gen[DataType] =
    for {
      depth <- Gen.choose(0, maxDepth)
      dt    <- if (depth == 0) leafDatatype else Gen.oneOf(genSchema(depth), genArray(depth), leafDatatype)
    } yield dt

  def genSchema(maxDepth: Int = 20): Gen[StructType] = {
    import scala.collection.JavaConverters._
    for {
      numberOfFields <- Gen.choose(1, 10)
      fieldNames     <- Gen.listOfN(numberOfFields, fieldNames)
      l = fieldNames.distinct.map(x => genDataType(maxDepth - 1).map(x -> _))
      fields <- Gen.sequence(l)
    } yield struct(fields.asScala: _*)
  }

  def genArray(maxDepth: Int): Gen[ArrayType] = genDataType(maxDepth - 1).map(x => ArrayType(x))

}
