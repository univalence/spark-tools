/*import cats.laws.discipline.MonadTests
import org.scalatest.{FunSuite, Matchers}
import org.typelevel.discipline.scalatest.Discipline

// Monad Laws
//tailRecM?


class Result extends FunSuite with Matchers with Discipline {
  checkAll("Result", MonadTests[].monad[Int, Int, Int])
}*/

import io.univalence._
import cats.instances.all._
import cats.laws.discipline.MonadTests
import org.scalatest.FunSuite


class ModelTest extends FunSuite{
  test("Monad Laws") {
    MonadTests[Option].monad[Int, Int, Int].all.check
  }
}