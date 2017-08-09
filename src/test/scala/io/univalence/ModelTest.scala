package io.univalence.centrifuge

import io.univalence.centrifuge._
import io.univalence.centrifuge.implicits._
import org.scalatest.FunSuite

import org.scalatest.{FunSuite, Matchers}
import org.typelevel.discipline.scalatest.Discipline

import io.univalence._
import cats.instances.all._
import cats.laws.discipline.CartesianTests.Isomorphisms
import cats.laws.discipline.MonadTests
import cats.laws.discipline.arbitrary._
import io.univalence.centrifuge.Result
import org.scalacheck.Arbitrary
import org.scalatest.FunSuite



class ModelTest extends FunSuite {
  val testAnnotation = Annotation("msg", Some("oF"), Vector("fF"), false, 1)
  val testResult = Result(Some("test"), Vector(testAnnotation))
  val testResultEmptyAnnotation = Result(Some("test"),Vector() )
  val testResultEmptyValue = Result(None,Vector(testAnnotation))
  val testResultBothEmpty = Result(None,Vector())

  test("isPure") {
    assert(testResult.isPure == false)
    assert(testResultEmptyValue.isPure == false)
    assert(testResultBothEmpty.isPure == false)
    assert(testResultEmptyAnnotation.isPure == true)

  }


  test("Monad laws") {

    import CatsContrib._

    implicit def abritrary[T](implicit arbitrary: Arbitrary[T]):Arbitrary[Result[T]] = {
      Arbitrary(arbitrary.arbitrary.map(Result.pure))
    }

    MonadTests[Result].monad[Int, Int, Int].all.check()

  }
}