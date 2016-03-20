
/**
 * Created by jon on 22/10/15.
 */

case class Ahoy(name: String, i: Int, l: String)

case class Result[T](nominal: Option[T], annotations: List[Any]) {

  def addPathPart(s: String): Result[T] = this
}


object testBuilder {


  //@autoBuildResult
  def build(name: Result[String],
            i: Result[Int],
            l: Result[String]): Result[Ahoy] = {

    val _1 = name.addPathPart("name")
    val _2 = i.addPathPart("i")
    val _3 = l.addPathPart("l")

    Result(nominal = (_1.nominal, _2.nominal, _3.nominal) match {
      case (Some(s1), Some(s2), Some(s3)) => Some(Ahoy(name = s1, i = s2, l = s3))
      case _ => None
    }, annotations = _1.annotations ::: _2.annotations ::: _3.annotations)
  }





}