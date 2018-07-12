package io.univalence.centrifuge

import org.scalatest.FunSuite

import scala.util.Try

class ExecutorTest extends FunSuite {

  def error: Nothing = throw new Exception("run error")

  test("breakafter") {

    val executor: Executor = Executor.build.name().breakAfter(2).getOrCreate()

    def s(e: ⇒ Any) = assert(executor.run(() ⇒ Try(e)).isSuccess)
    def f(e: ⇒ Any) = assert(!executor.run(() ⇒ Try(e)).isSuccess)

    s(1)
    f(error)
    s(2)
    f(error)
    f(error)
    f(3)
    f(4)
  }

  def failN[A](n: Int, a: A): () ⇒ Try[A] = {
    new Function0[Try[A]] {
      private var nbFailure: Int = n
      override def apply(): Try[A] = {
        if (nbFailure > 0) {
          nbFailure = nbFailure - 1
          Try { ??? }
        } else {
          Try(a)
        }
      }
    }
  }

  test("attempt") {

    val executor: Executor = Executor.build.name().attempt(2).getOrCreate()

    def s(e: ⇒ Any) = assert(executor.run(() ⇒ Try(e)).isSuccess)
    def f(e: ⇒ Any) = assert(!executor.run(() ⇒ Try(e)).isSuccess)

    //test failN
    {
      val f = failN(2, "value")
      assert(f().isFailure)
      assert(f().isFailure)
      assert(f().isSuccess)
    }

    val r1: () ⇒ Try[String] = failN(1, "value")
    s(r1().get)

    val r2 = failN(2, "value")
    f(r2().get)
    s(r2().get)

  }

  test("limitRate") {
    val executor: Executor = Executor.build.name().limitRate(100).getOrCreate()

    def s(e: ⇒ Any) = assert(executor.run(() ⇒ Try(e)).isSuccess)
    def f(e: ⇒ Any) = assert(!executor.run(() ⇒ Try(e)).isSuccess)

    val start = System.currentTimeMillis()
    (1 to 10).foreach(x ⇒ s(x))

    assert(System.currentTimeMillis() - start >= 100)
  }

  test("toto") {

    val executor: Executor = Executor.build.name().backoff(10).attempt(100).getOrCreate()

    def s(e: ⇒ Any) = assert(executor.run(() ⇒ Try(e)).isSuccess)
    def f(e: ⇒ Any) = assert(!executor.run(() ⇒ Try(e)).isSuccess)

    val start = System.currentTimeMillis()
    val r = failN(10, 1)
    s(r().get)
    assert(System.currentTimeMillis() - start >= 100)

  }

}
