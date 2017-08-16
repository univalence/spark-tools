package io.univalence.autobuild

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

    object Hello{
        @autoBuildResult
      def build(name : Result[String],
                greet : Result[Boolean]):Result[Hello] = MacroMarker.generated_applicative
    }
    

  }

}
