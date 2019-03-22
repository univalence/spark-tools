package io.univalence.autobuild

import io.univalence.centrifuge.Annotation
import io.univalence.centrifuge.Result
import org.scalatest.FunSuite

class Quickstart extends FunSuite {

  test("quickstart autobuild") {

    case class Hello(name: String, greet: Boolean)

    //Should not compile
    /*
    object Hello {
      @autoBuildResult
      def build:Result[Hello] = ???
    }
     */

    val name           = "Edgar Allan Poe"
    val pureStrResult  = Result.pure(name)
    val pureBoolResult = Result.pure(true)

    val errorAnnotation =
      Annotation.fromString(msg = "errorAnnotation", error = true)
    val regularAnnotation =
      Annotation.fromString(msg = "regularAnnotation", error = false)
    val errorStrResult  = Result(None, Vector(errorAnnotation))
    val errorBoolResult = Result(None, Vector(errorAnnotation))
    val regularStrResult =
      Result(Some("regularStrResult"), Vector(regularAnnotation))
    val regularBoolResult = Result(Some(true), Vector(regularAnnotation))

    object Hello {
      @autoBuildResult
      def build(
        name: Result[String],
        greet: Result[Boolean]
      ): Result[Hello] = MacroMarker.generated_applicative

    }

    // pure pure is pure
    assert(Hello.build(pureStrResult, pureBoolResult).isPure)

    //
    val errorBuild = Hello.build(errorStrResult, errorBoolResult)

    assert(errorBuild.annotations.size == 4)
    assert(errorBuild.isEmpty)
    assert(!errorBuild.isPure)
    assert(errorBuild.value == None)

    val warnBuild = Hello.build(regularStrResult, regularBoolResult)

    assert(warnBuild.annotations.size == 2)
    assert(!warnBuild.isEmpty)
    assert(!warnBuild.isPure)
    assert(warnBuild.value == Some(Hello("regularStrResult", true)))

    /*assert(warnBuild == Result(Some(Hello("regularStrResult", true)),
      Vector(Annotation("regularAnnotation", Some("nameregularAnnotation"), Vector("regularAnnotation"), false, 1),
        Annotation("regularAnnotation", Some("greetregularAnnotation"), Vector("regularAnnotation"), false, 1))))*/

    //print(Hello.build(errorStrResult, pureBoolResult))
  }

}
