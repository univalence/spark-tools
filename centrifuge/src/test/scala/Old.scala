import io.univalence.centrifuge.Result

case class Ahoy(name: String, i: Int, l: String)

object testBuilder {

  //@autoBuildResult
  def build(
      name: Result[String],
      i:    Result[Int],
      l:    Result[String]
  ): Result[Ahoy] = {

    val _1 = name.addPathPart("name")
    val _2 = i.addPathPart("i")
    val _3 = l.addPathPart("l")

    Result(
      value = (_1.value, _2.value, _3.value) match {
        case (Some(s1), Some(s2), Some(s3)) =>
          Some(Ahoy(name = s1, i = s2, l = s3))
        case _ => None
      },
      annotations = _1.annotations ++ _2.annotations ++ _3.annotations
    )
  }

}
