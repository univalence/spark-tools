package io.univalence.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import SchemaComparison._
import io.univalence.sparktest.DatatypeGen.ST
import org.scalacheck.{ Arbitrary, Shrink }
import org.scalatest.prop.PropertyChecks

import scala.util.{ Failure, Success, Try }

object SchemaBuilder {
  def double: DoubleType                            = DoubleType
  def integer: IntegerType                          = IntegerType
  def array(dataType: DataType): ArrayType          = ArrayType(dataType)
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
    val sc1 = struct("number" -> integer)
    assert(compareSchema(sc1, sc1) == Nil)
  }

  test("A field removed should return a SchemaModification RemoveField") {
    val sc1 = struct("number" -> integer, "name" -> integer)
    val sc2 = struct("number" -> integer)
    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"name", RemoveField(integer))))
  }

  test("A field added should return a SchemaModification with AddField") {
    val sc1 = struct("number" -> integer)
    val sc2 = struct("number" -> integer, "name" -> integer)

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"name", AddField(integer))))
  }

  test("A field changed should return a SchemaModification with ChangeField") {
    val sc1 = struct("number" -> integer)
    val sc2 = struct("number" -> double)

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(path"number", ChangeFieldType(integer, DoubleType))))
  }

  test(
    "A field removed, a field added, and a field changed should return a" +
      "SchemaModification with RemoveField, AddField, and ChangeField"
  ) {
    val sc1 = struct("number" -> integer, "name" -> integer)
    val sc2 = struct("rebmun" -> integer, "name" -> double)

    assert(
      compareSchema(sc1, sc2) == Seq(
        SchemaModification(path"number", RemoveField(integer)),
        SchemaModification(path"name", ChangeFieldType(integer, double)),
        SchemaModification(path"rebmun", AddField(integer))
      )
    )
  }

  test("Adding a field while the field exists should return a Duplicated Field error") {
    val sc = struct("number" -> integer)
    val sm = SchemaModification(path"number", AddField(integer))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(DuplicatedField("number"), sc, sm))
    )
  }

  test("Adding a field while the field not exists should return a Success with the new StructType") {
    val sc = struct("number" -> integer)
    val sm = SchemaModification(path"rebmun", AddField(integer))

    assert(
      modifySchema(sc, sm) ==
        Success(struct("number" -> integer, "rebmun" -> integer))
    )
  }

  test("Removing a field while the field is inexistant should return a Not Found Field error") {
    val sc = struct("number" -> integer)
    val sm = SchemaModification(path"name", RemoveField(integer))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm))
    )
  }

  test("Removing a field while the field is existant should return a Success with the new StructType") {
    val sc = struct("number" -> integer, "rebmun" -> integer)
    val sm = SchemaModification(path"rebmun", RemoveField(integer))

    assert(
      modifySchema(sc, sm) ==
        Success(struct(("number", integer)))
    )
  }

  test("Updating a field type while the field is inexistant should return a Not Found Field error") {
    val sc = struct("number" -> integer)
    val sm = SchemaModification(path"name", ChangeFieldType(integer, DoubleType))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm))
    )
  }

  test("Updating a field type while the field is existant should return a Success with the new StructType") {
    val sc = struct("number" -> integer)
    val sm = SchemaModification(path"number", ChangeFieldType(integer, double))

    assert(
      modifySchema(sc, sm) ==
        Success(struct(("number", DoubleType)))
    )
  }

  ignore("property base bug #1") {

    val s1 = struct("i" -> array(struct("o" -> struct("p" -> double))), "m" -> array(integer)) // 15 shrinks
    val s2 = struct("i" -> array(struct("v" -> integer))) // 12 shrinks

    assert(invariant(s1, s2))
  }

  def invariant(sc1: StructType, sc2: StructType): Boolean = {
    val diff = compareSchema(sc1, sc2)

    val sc3 = diff.foldLeft(Try(sc1))((sc, modif) => sc.flatMap(x => modifySchema(x, modif))).get

    val delta = compareSchema(sc3, sc2) // Should always be empty

    delta.isEmpty
  }

  ignore("test invariant") {

    implicit val ab: Arbitrary[ST] = Arbitrary(DatatypeGen.genSchema(5).map(ST.apply))

    import DatatypeGen._

    forAll { (s1: ST, s2: ST) =>
      invariant(s1.structType, s2.structType)
    }

  }
}

object DatatypeGen {

  case class ST(structType: StructType) {
    override def toString: String = {
      def toString(dataType: DataType): String =
        dataType match {
          case st: StructType => ST(st).toString
          case at: ArrayType => "array(" + toString(at.elementType) + ")"
          case _ => dataType.toString.toLowerCase.replace("type", "")
        }
      "struct(" + structType.fields.map(x => '"' + x.name + '"' + " -> " + toString(x.dataType)).mkString(", ") + ")"
    }
  }

  import org.scalacheck.Gen
  import SchemaBuilder._

  def sequenceStream[T](x: List[Stream[T]]): Stream[List[T]] =
    x match {
      case Nil => Stream(Nil)
      case a :: as =>
        for {
          v  <- a
          vv <- sequenceStream(as)
        } yield v :: vv
    }

  implicit def shrinkST: Shrink[ST] = Shrink(st => shrinkStructType.shrink(st.structType).map(ST.apply))

  implicit def shrinkStructType: Shrink[StructType] = Shrink[StructType](
    schema =>
      (for {
        n <- schema.fieldNames.indices.toStream
        if schema.fieldNames.length > 1
      } yield { StructType(schema.fields.filter(_.name != schema.fieldNames(n))) })
        ++ {
          sequenceStream(
            schema.fields.map(x => Shrink.shrinkWithOrig(x.dataType).map(dt => StructField(x.name, dt))).toList
          ).map(StructType.apply).filter(_ != schema)

      }
  )

  implicit def shrinkDatatype: Shrink[DataType] =
    Shrink[DataType]({
      case st: StructType => shrinkStructType.shrink(st)
      case dt: ArrayType  => Shrink.shrinkWithOrig(dt.elementType).map(x => ArrayType(x))
      case _              => Stream.empty
    })

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
