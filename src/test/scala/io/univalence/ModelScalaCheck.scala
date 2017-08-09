package io.univalence

import io.univalence.centrifuge.{Result, _}
import org.scalacheck.Prop._
import org.scalacheck.Properties

object ModelScalaCheck extends Properties("String") {

  property("isNotPure") = forAll { (a: String) =>
    Result(Some(a), Vector(Annotation("msg", Some("oF"), Vector("fF"), false, 1))).isPure == false
    Result(None, Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).isPure == false
    Result(Some(a), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).isPure == false
    Result(Some("msg"), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).isPure == false
  }
  property("isPure") = forAll { (a: String) =>
    Result(Some(a), Vector()).isPure == true
  }
  property("filter") = forAll { (a: String) =>
    Result(Some(a), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).filter(_.contains(a)) == Result(
      Some(a), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1)))
  }

}