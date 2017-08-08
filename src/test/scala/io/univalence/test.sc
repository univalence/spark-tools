import cats._, cats.data._, cats.implicits._, cats.laws.discipline.MonadTests

val rs = MonadTests[Option].monad[Int, Int, Int]

rs.all.check

