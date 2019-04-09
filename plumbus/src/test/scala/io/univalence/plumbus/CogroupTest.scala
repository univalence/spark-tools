package io.univalence.plumbus
import io.univalence.plumbus.test.SparkTestLike
import org.apache.spark.sql.Dataset
import org.scalatest.{ FunSuiteLike, Matchers }
import com.github.mrpowers.spark.fast.tests.DatasetComparer

class CogroupTest extends FunSuiteLike with SparkTestLike with Matchers with DatasetComparer {
  import spark.implicits._
  import io.univalence.plumbus.cogroup._

  val person1 = PersonWithId("1", "John", 32)
  val person2 = PersonWithId("2", "Mary", 32)

  val address1 = Address("1", "address1")
  val address2 = Address("2", "address2")
  val address3 = Address("1", "address3")

  val persons: Dataset[PersonWithId] = Seq(person1, person2).toDS()
  val addresses: Dataset[Address]    = Seq(address1, address2, address3).toDS()

  test("apply test") {
    val applyDS = apply(persons, addresses)(_.id, _.idPerson)
    val expectedDS = Seq(
      ("1", Seq(person1), Seq(address1, address3)),
      ("2", Seq(person2), Seq(address2))
    ).toDS()
    assertSmallDatasetEquality(applyDS, expectedDS, orderedComparison = false)
  }
}

case class Address(idPerson: String, name: String)
