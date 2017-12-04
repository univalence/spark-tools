/*package psug.part4

import io.univalence.autobuild.autoBuildResult


case class HelloHello(name: String, i: Int)


case class Annotation(position: String, error: String) {
  def addPathPath(s: String): Annotation = this.copy(position = s + position)
}


case class Result[T](nominal: Option[T], annotations: List[Annotation]) {
  def addPathPart(s: String): Result[T] = this.copy(annotations = annotations.map(_.addPathPath(s)))
}

object Result {


  def point[A](a:A) = Result(nominal = Some(a), Nil)

  implicit class ROps[A](ra: Result[A]) {
    def app[B](f: Result[A => B]): Result[B] = ???

    def map[B](f: A => B): Result[B] = ???

  }

}


object HelloHello {


  @autoBuildResult
  def build(name: Result[String],
            i: Result[Int]): Result[HelloHello] = {

    val _1 = name.addPathPart("name")
    val _2 = i.addPathPart("i")

    Result(nominal = (_1.nominal, _2.nominal) match {
      case (Some(s1), Some(s2)) => Some(HelloHello(name = s1, i = s2))
      case _ => None
    }, annotations = _1.annotations ::: _2.annotations)
  }
}

object TestTest {

  def main(args: Array[String]) {

    import io.univalence.excelsius._
    HelloHello.build(Result(None, Nil), Result(Some(1), List(Annotation("", "c'est pas bon")))).excel()
  }
}*/ 