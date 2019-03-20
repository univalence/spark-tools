package io.univalence

import io.univalence.centrifuge.Result
import io.univalence.centrifuge._
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop.all
import org.scalacheck.Properties

object ModelScalaCheck extends Properties("String") {

  property("isNotPure") = forAll { a: String =>
    all(
      !Result(Some(a), Vector(Annotation("msg", Some("oF"), Vector("fF"), false, 1))).isPure,
      !Result(None, Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).isPure,
      !Result(Some(a), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).isPure,
      !Result(Some("msg"), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))).isPure
    )
  }
  property("isPure") = forAll { a: String =>
    Result(Some(a), Vector()).isPure
  }
  property("filter") = forAll { a: String =>
    Result(Some(a), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1)))
      .filter(_.contains(a)) == Result(
      Some(a),
      Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))
    )
  }
  property("map") = forAll { a: String =>
    all(
      Result(Some(a), Vector(Annotation("msg", Some("oF"), Vector("fF"), false, 1)))
        .map(_.toString) == Result(
        Some(a),
        Vector(Annotation("msg", Some("oF"), Vector("fF"), false, 1))
      ),
      Result(Some(a), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1)))
        .map(_.toString) == Result(
        Some(a),
        Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))
      ),
      Result(Some("msg"), Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))) == Result(
        Some("msg"),
        Vector(Annotation(a, Some("oF"), Vector("fF"), false, 1))
      )
    )
  }

}
