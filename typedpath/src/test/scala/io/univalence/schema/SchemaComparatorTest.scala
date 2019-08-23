package io.univalence.schema

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import SchemaComparator._
import io.univalence.schema.DatatypeGen.ST
import org.scalacheck.{Arbitrary, Shrink}
import org.scalatest.prop.PropertyChecks

import scala.util.{Failure, Success}

case class DtAndNull(dt: DataType, nullable: Boolean = true)

object SchemaBuilder {
  def string: StringType                   = StringType
  def double: DoubleType                   = DoubleType
  def integer: IntegerType                 = IntegerType
  def array(dataType: DataType): ArrayType = ArrayType(dataType)
  def struct(args: (String, DtAndNull)*): StructType =
    StructType(args.map({ case (k, DtAndNull(d, b)) => StructField(k, d, b) }))
}

class SchemaComparatorTest extends FunSuite with PropertyChecks {
  import io.univalence.typedpath._

  import SchemaBuilder._

  val ss: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.ui.enabled", value = false)
    .getOrCreate()

  val sc: SparkContext = ss.sparkContext

  test("The two schemas are empty") {
    val sc1: StructType = struct()
    assert(compareSchema(sc1, sc1) == Nil)
  }

  test("Two identical schema have no schema modification") {
    val sc1 = struct("number" -> DtAndNull(integer))
    assert(compareSchema(sc1, sc1) == Nil)
  }

  test("A field removed should return a SchemaModification RemoveField") {
    val sc1 = struct("number" -> DtAndNull(integer), "name" -> DtAndNull(integer))
    val sc2 = struct("number" -> DtAndNull(integer))
    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(key"name", RemoveField(integer))))
  }

  test("A field added should return a SchemaModification with AddField") {
    val sc1 = struct("number" -> DtAndNull(integer))
    val sc2 = struct("number" -> DtAndNull(integer), "name" -> DtAndNull(integer))

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(key"name", AddField(integer))))
  }

  test("A field changed should return a SchemaModification with ChangeField") {
    val sc1 = struct("number" -> DtAndNull(integer))
    val sc2 = struct("number" -> DtAndNull(double))

    assert(compareSchema(sc1, sc2) == Seq(SchemaModification(key"number", ChangeFieldType(integer, DoubleType))))
  }

  test(
    "A field removed, a field added, and a field changed should return a" +
      "SchemaModification with RemoveField, AddField, and ChangeField"
  ) {
    val sc1 = struct("number" -> DtAndNull(integer), "name" -> DtAndNull(integer))
    val sc2 = struct("rebmun" -> DtAndNull(integer), "name" -> DtAndNull(double))

    assert(
      compareSchema(sc1, sc2) == Seq(
        SchemaModification(key"number", RemoveField(integer)),
        SchemaModification(key"name", ChangeFieldType(integer, double)),
        SchemaModification(key"rebmun", AddField(integer))
      )
    )
  }

  test("Adding a field while the field exists should return a Duplicated Field error") {
    val sc = struct("number" -> DtAndNull(integer))
    val sm = SchemaModification(key"number", AddField(integer))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(DuplicatedField("number"), sc, sm))
    )
  }

  test("Adding a field while the field not exists should return a Success with the new StructType") {
    val sc = struct("number" -> DtAndNull(integer))
    val sm = SchemaModification(key"rebmun", AddField(integer))

    assert(
      modifySchema(sc, sm) ==
        Success(struct("number" -> DtAndNull(integer), "rebmun" -> DtAndNull(integer)))
    )
  }

  test("Removing a field while the field is inexistant should return a Not Found Field error") {
    val sc = struct("number" -> DtAndNull(integer))
    val sm = SchemaModification(key"name", RemoveField(integer))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm))
    )
  }

  test("Removing a field while the field is existant should return a Success with the new StructType") {
    val sc = struct("number" -> DtAndNull(integer), "rebmun" -> DtAndNull(integer))
    val sm = SchemaModification(key"rebmun", RemoveField(integer))

    assert(
      modifySchema(sc, sm) ==
        Success(struct(("number", DtAndNull(integer))))
    )
  }

  test("Updating a field type while the field is inexistant should return a Not Found Field error") {
    val sc = struct("number" -> DtAndNull(integer))
    val sm = SchemaModification(key"name", ChangeFieldType(integer, DoubleType))

    assert(
      modifySchema(sc, sm) ==
        Failure(ApplyModificationErrorWithSource(NotFoundField("name"), sc, sm))
    )
  }

  test("Updating a field type while the field is existant should return a Success with the new StructType") {
    val sc = struct("number" -> DtAndNull(integer))
    val sm = SchemaModification(key"number", ChangeFieldType(integer, double))

    assert(
      modifySchema(sc, sm) ==
        Success(struct(("number", DtAndNull(DoubleType))))
    )
  }

  test("property base bug #1") {
    val s1 = struct("i" -> DtAndNull(array(struct("o" -> DtAndNull(struct("p" -> DtAndNull(double)))))),
                    "m" -> DtAndNull(array(integer))) // 15 shrinks
    val s2 = struct("i" -> DtAndNull(array(struct("v" -> DtAndNull(integer))))) // 12 shrinks

    assertInvariant(s1, s2)
  }

  test("property base bug #2") {
    val s1 = struct("k" -> DtAndNull(array(integer))) // shrinked manualy
    val s2 = struct("k" -> DtAndNull(array(string))) // 5 shrinks

    assertInvariant(s1, s2)
  }

  test("property base bug #3") {
    val arg0 = struct("t" -> DtAndNull(double), "f" -> DtAndNull(array(string))) // 9 shrinks
    val arg1 = struct("v" -> DtAndNull(double), "f" -> DtAndNull(double)) // 4 shrinks

    assertInvariant(arg0, arg1)
  }

  test("property base bug #4") {
    val arg0 = struct("e" -> DtAndNull(array(array(struct("w" -> DtAndNull(string)))))) // 12 shrinks + Manualy
    val arg1 = struct("e" -> DtAndNull(array(string))) // 4 shrinks

    assertInvariant(arg0, arg1)
  }

  test("property base bug #5") {
    val arg0 = struct("e" -> DtAndNull(double, nullable = false))
    val arg1 = struct("e" -> DtAndNull(double, nullable = true))

    assertInvariant(arg0, arg1)
    assertInvariant(arg1, arg0)
  }

  def assertInvariant(s1: StructType, s2: StructType): Unit = {
    val diff = compareSchema(s1, s2)

    var s3 = s1
    diff.foreach(sm => {
      val t = modifySchema(s3, sm)
      if (t.isFailure) println(sm)
      s3 = t.get
    })

    assertSchemaEquals(s3, s2)
  }

  def assertDatatypeEquals(d1: DataType, d2: DataType): Unit =
    (d1, d2) match {
      case (s1: StructType, s2: StructType) => assertSchemaEquals(s1, s2)
      case _                                => assert(d1 == d2)
    }

  def assertSchemaEquals(s1: StructType, s2: StructType): Unit =
    for {
      name <- (s2.fieldNames ++ s1.fieldNames).distinct
    } {
      (s1.fields.find(_.name == name), s2.fields.find(_.name == name)) match {
        case (Some(f1), Some(f2)) => assertDatatypeEquals(f1.dataType, f2.dataType)
        case (Some(f1), None)     => throw new Exception(s"extra field : $f1")
        case (None, Some(f1))     => throw new Exception(s"extra field : $f1")

      }

    }

  test("test invariant") {

    implicit val ab: Arbitrary[ST] = Arbitrary(DatatypeGen.genSchema(5).map(ST.apply))

    import DatatypeGen._

    forAll { (s1: ST, s2: ST) =>
      assertInvariant(s1.structType, s2.structType)
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

  def fieldNames: Gen[String] = Gen.oneOf("abcdefghijklm".toSeq.map(_.toString))

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
    } yield struct(fields.asScala.map { case (n, d) => (n, DtAndNull(d)) }: _*)
  }

  def genArray(maxDepth: Int): Gen[ArrayType] = genDataType(maxDepth - 1).map(x => ArrayType(x))

}
