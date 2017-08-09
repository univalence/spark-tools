package io.univalence.centrifuge

import io.univalence.centrifuge._
import io.univalence.centrifuge.implicits._
import org.scalatest.FunSuite

/*import cats.laws.discipline.MonadTests
import org.scalatest.{FunSuite, Matchers}
import org.typelevel.discipline.scalatest.Discipline

// Monad Laws
//tailRecM?


class Result extends FunSuite with Matchers with Discipline {
  checkAll("Result", MonadTests[].monad[Int, Int, Int])
}*/

import io.univalence._
/*import cats.instances.all._
import cats.laws.discipline.MonadTests
import org.scalatest.FunSuite


class ModelTest extends FunSuite{
  test("Monad Laws") {
    MonadTests[Option].monad[Int, Int, Int].all.check()
  }
}*/

class ModelTest extends FunSuite {
  val testAnnotation = Annotation("msg", Some("oF"), Vector("fF"), false, 1)
  val testResult = Result(Some("test"), Vector(testAnnotation))
  val testResultEmptyAnnotation = Result(Some("test"),Vector() )
  val testResultEmptyValue = Result(None,Vector(testAnnotation))
  val testResultBothEmpty = Result(None,Vector())

  test("isPure"){
    assert(testResult.isPure == false)
    assert(testResultEmptyValue.isPure == false)
    assert(testResultBothEmpty.isPure == false)
    assert(testResultEmptyAnnotation.isPure == true)
  }
}