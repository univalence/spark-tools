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
    val pureStrResult = Result(Some(name),Vector())
    val pureBoolResult = Result(Some(true),Vector())
    val errorAnnotation = Annotation("errorAnnotation", Some("errorAnnotation"), Vector("errorAnnotation"), true, 1)
    val regularAnnotation = Annotation("regularAnnotation", Some("regularAnnotation"), Vector("regularAnnotation"),
      false, 1)
    val errorStrResult = Result(Some("errorStrResult"), Vector(errorAnnotation))
    val errorBoolResult = Result(Some(true), Vector(errorAnnotation))
    val regularStrResult = Result(Some("regularStrResult"), Vector(regularAnnotation))
    val regularBoolResult = Result(Some(true), Vector(regularAnnotation))

    object Hello{
      @autoBuildResult
      def build(name : Result[String],
                greet : Result[Boolean]):Result[Hello] = MacroMarker.generated_applicative

    }


    // pure pure is pure
    assert(Hello.build(pureStrResult, pureBoolResult).isPure)

    //
    assert(Hello.build(errorStrResult, errorBoolResult) == Result(Some(Hello("errorStrResult",true)),
      Vector(Annotation("errorAnnotation",Some("nameerrorAnnotation"),Vector("errorAnnotation"),true,1),
        Annotation("errorAnnotation",Some("greeterrorAnnotation"),Vector("errorAnnotation"),true,1))))

    //
    assert(Hello.build(regularStrResult, regularBoolResult) == Result(Some(Hello("regularStrResult",true)),
      Vector(Annotation("regularAnnotation",Some("nameregularAnnotation"),Vector("regularAnnotation"),false,1),
        Annotation("regularAnnotation",Some("greetregularAnnotation"),Vector("regularAnnotation"),false,1))))

    print(Hello.build(errorStrResult, pureBoolResult))
  }

}
