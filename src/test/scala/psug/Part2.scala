package psug.part2

import scala.language.higherKinds
import scalaz.Applicative

case class Hello(name: String, i: Int)

object Hello {

  def build[A[_]: Applicative](name: A[String], i: A[Int]): A[Hello] = {
    import scalaz.syntax.applicative._
    (name |@| i)(Hello.apply)

  }

  def main(args: Array[String]) {
    import scalaz.std.option._

    println(build(name = Option("ahoy"), i = Option(-1)))

  }
}
