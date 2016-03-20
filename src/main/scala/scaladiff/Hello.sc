import io.univalence.autobuild._

CaseClassApplicativeBuilder.generateDef(List("name"
  -> "String", "a" -> "Int", "b" -> "Double"), "Ahoy", "Option").all


case class Ahoy(name: String, a: Int, b: Double)

implicit class WrapOption[T](o: Option[T]) {

  def app[B](tb: Option[T => B]): Option[B] = {
    tb.flatMap(o.map)
  }
}


def build(name: Option[String],
          a: Option[Int],
          b: Option[Double]): Option[Ahoy] = {
  def zip[A, B](x: A)(y: B): (A, B) = (x, y)
  def zipC[A, B](x: Option[A])(y: Option[B]): Option[(A, B)] = y.app(x.map(zip))
  zipC(b)(zipC(a)(name)).map(t => Ahoy(name = t._2._2, b = t._1, a = t._2._1))
}