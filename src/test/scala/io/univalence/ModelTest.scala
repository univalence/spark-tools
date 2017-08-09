package io.univalence.centrifuge

import cats.instances.all._
import cats.laws.discipline.MonadTests
import io.univalence._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite

import scala.util.{Failure, Success}


class ModelTest extends FunSuite {
  val testAnnotation = Annotation("msg", Some("oF"), Vector("fF"), false, 1)
  val testResult = Result(Some("msg"), Vector(testAnnotation))
  val testResultEmptyAnnotation = Result(Some("msg"), Vector())
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

  test("mapAnnotations"){
    //assert(testResult.mapAnnotations() == Result(Some("msg"), Vector(Annotation("msg", Some("oF"), Vector("fF"), false, 1))))
  }

  test("addPathPart"){}

  test("hasAnnotations"){
    assert(testResult.hasAnnotations == true)
    assert(testResultEmptyValue.hasAnnotations == true)
    assert(testResultEmptyAnnotation.hasAnnotations == false)
    assert(testResultBothEmpty.hasAnnotations == false)
  }

  test("map"){
    assert(testResult.map(_.toUpperCase) == Result(Some("MSG"), Vector(testAnnotation)))
  }


  test("map2"){}


  test("filter"){
    assert(testResult.filter(_.startsWith("m")) == testResult)
    //assert(testResult.filter(_.startsWith("a")) == testResult)
  }

  test("get"){
    assert(testResult.get == "msg")

    assert(testResultEmptyAnnotation.get == "msg")

    val throwEmptyValue = intercept[Exception]{
      testResultEmptyValue.get
    }

    assert(throwEmptyValue.getMessage == "empty result : Annotation(msg,Some(oF),Vector(fF),false,1)")

    val throwBothEmpty = intercept[Exception]{
      testResultBothEmpty.get
    }
    assert(throwBothEmpty.getMessage == "empty result : ")
  }

  //TODO
  test("toTry"){
    assert(testResult.toTry == Success("msg"))
    /*val throwEmptyValue = intercept[Throwable]{
      testResultEmptyValue.toTry
    }
    assert(testResultEmptyValue.toTry == Failure(throwEmptyValue))*/
  }

  test("toEither"){

  }

  test("fromTry"){}

  test("fromEither"){}
}