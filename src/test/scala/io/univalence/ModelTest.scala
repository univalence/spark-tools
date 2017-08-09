package io.univalence.centrifuge

import cats.instances.all._
import cats.laws.discipline.MonadTests
import io.univalence._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite


class ModelTest extends FunSuite {
  val testAnnotation = Annotation("msg", Some("oF"), Vector("fF"), false, 1)
  val testResult = Result(Some("test"), Vector(testAnnotation))
  val testResultEmptyAnnotation = Result(Some("test"), Vector())
  val testResultEmptyValue = Result(None, Vector(testAnnotation))
  val testResultBothEmpty = Result(None, Vector())

  test("isPure") {
    assert(testResult.isPure == false)
    assert(testResultEmptyValue.isPure == false)
    assert(testResultBothEmpty.isPure == false)
    assert(testResultEmptyAnnotation.isPure == true)

  }

  import CatsContrib._

  implicit val arbitraryAnn: Arbitrary[Annotation] = Arbitrary(Gen.resultOf(Annotation.apply _))

  implicit def arbitraryResult[T](implicit oA: Arbitrary[Option[T]],
                                  aAnn: Arbitrary[Vector[Annotation]]): Arbitrary[Result[T]] = {
    Arbitrary(Gen.resultOf[Option[T], Vector[Annotation], Result[T]](Result.apply))
  }


  test("Monad laws") {
    MonadTests[Result].stackUnsafeMonad[Int, Int, Int].all.check()
  }

  ignore("Should be stacksafe") {
    MonadTests[Result].monad[Int, Int, Int].all.check()
  }

  test("mapAnnotations"){}

  test("addPathPart"){}

  test("hasAnnotations"){
    assert(testResult.hasAnnotations == true)
    assert(testResultEmptyValue.hasAnnotations == true)
    assert(testResultEmptyAnnotation.hasAnnotations == false)
    assert(testResultBothEmpty.hasAnnotations == false)
  }

  test("map"){
  }

  test("map2"){}

  test("flatMap"){}

  test("filter"){
  }

  test("get"){

  }

  test("toTry"){}

  test("toEither"){}

  test("fromTry"){}

  test("fromEither"){}
}