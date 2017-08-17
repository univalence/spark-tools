package io.univalence.autobuild

import io.univalence.centrifuge.{Annotation, Result}
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

    val name = "Edgar Allan Poe"
    val pureStrResult = Result.pure(name)
    val pureBoolResult = Result.pure(true)

    val errorAnnotation = Annotation.fromString(msg = "errorAnnotation", error = true)
    val regularAnnotation = Annotation.fromString(msg = "regularAnnotation", error = false)
    val errorStrResult = Result(None, Vector(errorAnnotation))
    val errorBoolResult = Result(None, Vector(errorAnnotation))
    val regularStrResult = Result(Some("regularStrResult"), Vector(regularAnnotation))
    val regularBoolResult = Result(Some(true), Vector(regularAnnotation))

    object Hello {
      @autoBuildResult
      def build(name: Result[String],
                greet: Result[Boolean]): Result[Hello] = MacroMarker.generated_applicative

    }


    // pure pure is pure
    assert(Hello.build(pureStrResult, pureBoolResult).isPure)

    //
    val errorBuid = Hello.build(errorStrResult, errorBoolResult)

    assert(errorBuid.annotations.size == 4)
    assert(errorBuid.isEmpty)


    val warnBuild = Hello.build(regularStrResult, regularBoolResult)



    assert(warnBuild == Result(Some(Hello("regularStrResult", true)),
      Vector(Annotation("regularAnnotation", Some("nameregularAnnotation"), Vector("regularAnnotation"), false, 1),
        Annotation("regularAnnotation", Some("greetregularAnnotation"), Vector("regularAnnotation"), false, 1))))

    //print(Hello.build(errorStrResult, pureBoolResult))
  }

}
